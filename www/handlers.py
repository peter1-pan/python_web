import re, time, json, logging, hashlib, base64, asyncio

import markdown
from aiohttp import web
from models import User, Comment, Blog, next_id
from coroweb import get, post
## 分页管理以及调取API时的错误信息
from apis import Page, APIValueError, APIResourceNotFoundError
from config import configs
'''
后端API包括：

    获取日志：GET /api/blogs
    创建日志：POST /api/blogs
    修改日志：POST /api/blogs/:blog_id
    删除日志：POST /api/blogs/:blog_id/delete
    获取评论：GET /api/comments
    创建评论：POST /api/blogs/:blog_id/comments
    删除评论：POST /api/comments/:comment_id/delete
    创建新用户：POST /api/users
    获取用户：GET /api/users

管理页面包括：

    评论列表页：GET /manage/comments
    日志列表页：GET /manage/blogs
    创建日志页：GET /manage/blogs/create
    修改日志页：GET /manage/blogs/
    用户列表页：GET /manage/users

用户浏览页面包括：

    注册页：GET /register
    登录页：GET /signin
    注销页：GET /signout
    首页：GET /
    日志详情页：GET /blog/:blog_id
'''
COOKIE_NAME = 'awesession'
_COOKIE_KEY = configs.session.secret

## 查看是否是管理员用户
def check_admin(request):
    if request.__user__ is None or not request.__user__.admin:
        raise APIPermissionError()

## 获取页码信息
def get_page_index(page_str):
    pass

## 计算加密cookie
def user2cookie(user, max_age):
    # build cookie string by: id-expires-sha1
    expires = str(int(time.time() + max_age))
    s = '{}-{}-{}-{}'.format(user.id, user.passwd, expires, _COOKIE_KEY)
    L = [user.id, expires, hashlib.sha1(s.encode('utf-8')).hexdigest()]
    return '-'.join(L)

## 文本转html
def text2html(text):
    pass

# 解密cookie
async def cookie2user(cookie_str):
    if not cookie_str:
        return None
    try:
        L = cookie_str.split('-')
        if len(L) != 3:
            return None
        uid, expires, sha1 = L
        if int(expires) < time.time():
            return None
        user = await User.find(uid)
        if user is None:
            return None
        s = '{}-{}-{}-{}'.format(uid, user.passwd, expires, _COOKIE_KEY)
        if sha1 != hashlib.sha1(s.encode('utf-8')).hexdigest():
            logging.info('Invalid sha1')
            return None
        user.passwd = '******'
        return user
    except Exception as e:
        logging.exception(e)
        return None

## 定义EMAIL和HASH的格式规范
_RE_EMAIL = re.compile(r'^[a-z0-9\.\-\_]+\@[a-z0-9\-\_]+(\.[a-z0-9\-\_]+){1,4}$')
_RE_SHA1 = re.compile(r'^[0-9a-f]{40}$')

## 处理注册页面URL
@get('/register')
def register():
    return {
        '__template__': 'register.html'
    }

## 处理登陆页面URL
@get('/signin')
def signin():
    return {
        '__template__': 'signin.html'
    }

## 用户注册API
@post('/api/users')
async def api_register_user(*, email, name, passwd):
    if not name or not name.strip():
        raise APIValueError('name')
    if not email or not _RE_EMAIL.match(email):
        raise APIValueError('email')
    if not passwd or not _RE_SHA1.match(passwd):
        raise APIValueError('passwd')
    users = await User.findall('email=?', [email])
    if len(users) > 0:
        raise APIValueError('register:failed', 'email', 'Email is already in use.')
    uid = next_id()
    sha1_passwd = '{}:{}'.format(uid, passwd)
    user = User(id=uid,name=name.strip(),email=email,passwd=hashlib.sha1(sha1_passwd.encode('utf-8')).hexdigest(),image='http://www.gravatar.com/avatar/%s?d=mm&s=120' % hashlib.md5(email.encode('utf-8')).hexdigest())
    print('create user: ', user)
    await user.save()
    print('*************存入成功****************')
    # make session cookie
    r = web.Response()
    r.set_cookie(COOKIE_NAME,user2cookie(user,86400),max_age=86400,httponly=True)
    user.passwd = '******'
    r.content_type = 'application/json'
    r.body = json.dumps(user, ensure_ascii=False).encode('utf-8')
    return r

## 用户登陆验证API
@post('/api/authenticate')
async def authenticate(*, email, passwd):
    if not email:
        raise APIValueError('email', 'Invalid email.')
    if not passwd:
        raise APIValueError('passwd', 'Invalid password.')
    users = await User.findall('email=?', [email])
    if len(users) == 0:
        raise APIValueError('email', 'Email not exist.')
    user = users[0]
    print('user: ',user)
    # check passwd
    sha1 = hashlib.sha1()
    sha1.update(user.id.encode('utf-8'))
    sha1.update(b':')
    sha1.update(passwd.encode('utf-8'))
    if user.passwd != sha1.hexdigest():
        print('hexdigest: ', sha1.hexdigest())
        print('check passwd: ',user.passwd)
        raise APIValueError('passwd', 'Invalid password.')
    # authenticate ok, set cookie
    r = web.Response()
    r.set_cookie(COOKIE_NAME,user2cookie(user,86400),max_age=86400,httponly=True)
    user.passwd = '******'
    r.content_type = 'application/json'
    r.body = json.dumps(user, ensure_ascii=False).encode('utf-8')
    return r




@get('/blogs')
async def index2(request):
    summary = 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'
    blogs = [
        Blog(id='1', name='Test Blog', summary=summary, created_at=time.time()-120),
        Blog(id='2', name='Something New', summary=summary, created_at=time.time()-3600),
        Blog(id='3', name='Learn Swift', summary=summary, created_at=time.time()-7200)
    ]
    return {
        '__template__': 'blogs.html',
        'blogs': blogs
    }
    
## 处理首页URL
@get('/')
async def index(request):
    users = await User.findall()
    return {
        '__template__': 'test.html',
        'users': users
    }

## 处理日志详情页面URL




@get('/api/user')
async def api_get_users():
    users = await User.findall(orderBy='created_at desc')
    for u in users:
        u.passwd = '******'
    return dict(users=users)

