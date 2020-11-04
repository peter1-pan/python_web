import asyncio, logging, aiomysql

# 创建基本日志函数
def log(sql, args=()):
    logging.info('SQL: {}'.format(sql))

# 异步IO起手式，创建连接池函数，pool用法如下
# https://aiomysql.readthedocs.io/en/latest/pool.html?highlight=create_pool
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    # 声明__pool为全局变量
    global __pool
    # 使用这些基本参数创建连接池
    # await和async是联动的（异步IO）
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

async def select(sql, args, size=None):
    log(sql, args)
    global __pool

    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor) # DictCursor要求返回的是一个字典
        # cursor实例可以调用execute来执行一条单独的SQL语句，参考
        # https://docs.python.org/zh-cn/3.8/library/sqlite3.html?highlight=execute#cursor-objects
        # sql语句的占位符是'?',MySQL的占位符是'%s'
        await cur.execute(sql.replace('?', '%s'), args or())
        if size:
            # fetchmany可以获取指定行数为size的多行查询结果,返回一个列表
            rs = await cur.fetchmany(size)
        else:
            # fetchall可以获取一个查询结果的所有行,返回一个列表
            rs = await cur.fetchall()
    await cur.close()
    # 日志: 提示返回了多少行
    logging.info('rows returned: {}'.format(len(rs)))
    return res  # 返回的是一个列表

# 通用函数, 返回INSERT、UPDATE、DELETE这3种SQL的执行所影响的行数
async def execute(sql, args):
    log(sql)
    global __pool

    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            # rowcount获取行数
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
    return affected  # 返回表示影响的行数

def create_args_string(num):
    L = []
    for _ in range(num):
        L.append('?')
    return ', '.join(L) 

class ModelMetaclass(type):
    # cls: 当前准备创建类的对象 class
    # name: 类的名字 str
    # bases: 类继承的父类集合 Tuple
    # attrs: 类的方法集合
    def __new__(cls, name, bases, attrs):
        # 排出Model类本身,返回它自己
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称
        tableName = attrs.get('__table__', None) or name
        # 日志: 找到名为name的model
        logging.info('found model: {} (table: {})'.format(name, tableName))
        # 获取所有的Field和主键名
        mappings = dict()
        fields = []
        primaryKey = None

        for k,v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: {} ==> {}'.format(k,v))
                mappings[k] = v
                # 如果v.primary_key为True,这个field为主键
                if v.primary_key:
                    if primaryKey:
                        # 找到主键,如果主键primaryKey有值时，返回一个错误
                        raise RuntimeError('Duplicate primary key for field: {}'.format(k))
                    # 给主键赋值
                    primaryKey = k
                else:
                    # 没找到主键，在fields里添加k
                    fields.append(k)
        if not primaryKey:
            # 如果主键为None就报错
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)

        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName # table 名
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的 SELECT, INSERT, UPDAT E和 DELETE 语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        # super(Model, self)首先找到Model的父类(即ModelMetaclass)
        # 然后把类Model的对象转换为类ModelMetaclass的对象
        super(Model, self).__init__(**kw)
    
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '{}' ".format(key))
    
    def __setattr__(self, key, value):
        self[key] = value

    # 通过属性返回想要的值
    def getValue(self, key):
        return getattr(self, key, None)
    
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for {}: {}'.format(key,str(value)))
                setattr(self, key, value)
        return value

    # *** 往Model类添加class方法，可以让所有子类调用class方法
    @classmethod
    async def findall(cls, where=None, args=None, **kw):
        sql = [cls.__select__]
        # 如果where有值就在sql加上字符串'where'和变量where
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            # 如果findAll函数未传入有效的where参数，就将'[]'传入args
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invaild limit value: {}'.format(str(limit)))
        rs = await select(' '.join(sql), args)
        # 完成选择的列表里的所有值,完成findAll函数
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['__num__']

    @classmethod
    async def find(cls, pk):
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    # *** 往Model类添加实例方法，可以让所有子类调用实例方法
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: {}'.format(rows))

    async def update(self):
        args = list(map(self.getValue, self.__fields__))        
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: {}'.format(rows))
    
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: {}'.format(rows))


class Field:
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    
    def __str__(self):
        return '<{}, {}:{}>'.format(self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)