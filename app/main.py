import datetime

from models import engine, Session
from getter import SWAPIGetter


if __name__ == '__main__':
    getter = SWAPIGetter(chunk_size=100, engine=engine, session=Session)

    start = datetime.datetime.now()
    getter.get_and_download_to_db(26, 70)
    print(datetime.datetime.now() - start)
