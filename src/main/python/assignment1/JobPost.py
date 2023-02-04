from datetime import datetime
import calendar


class JobPost(object):

    def __init__(self):
        self.job_title = ''
        self.company_name = ''
        self.company_rating = ''
        self.salary = ''
        self.timestamp_ms = calendar.timegm(datetime.utcnow().utctimetuple())
