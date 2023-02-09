import datetime

from models import engine, Session
from getter import SWAPIExtractor


if __name__ == '__main__':
    extractor = SWAPIExtractor(chunk_size=100, engine=engine, session=Session)

    start = datetime.datetime.now()
    extractor.extract_and_download_to_db(26, 70)
    print(datetime.datetime.now() - start)
