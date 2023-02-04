from bs4 import BeautifulSoup
from JobPost import JobPost


def parse_html(stream):
    try:
        soup = BeautifulSoup(stream, features="html.parser")
        job_posts_lst = list()
        for table in soup.find_all('table', class_='jobCard_mainContent'):
            all_spans = table.find_all('span')
            new_job_post = JobPost()
            for span in all_spans:
                # parse Job Title
                if span.get('title'):
                    new_job_post.job_title = span.get('title')
                # parse Company Name
                if span.get('class') and 'companyName' in span.get('class'):
                    new_job_post.company_name = span.get_text()
                # parse Rating Number
                if span.get('class') and 'ratingNumber' in span.get('class'):
                    new_job_post.company_rating = span.get_text()
                # parse Salary
                if span.get('class') and 'salary-snippet' in span.get('class'):
                    new_job_post.salary = span.get_text()
            job_posts_lst.append(new_job_post.__dict__)
        return job_posts_lst
    except Exception as e:
        raise
