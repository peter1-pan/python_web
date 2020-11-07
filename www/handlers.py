from models import User
from coroweb import get
import asyncio


@get('/')
async def index(request):
    users = await User.findall()
    return {
        '__template__': 'test.html',
        'users': users
    }