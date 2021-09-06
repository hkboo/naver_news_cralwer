# Crawler Package
import urllib.request as req
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time

# Data Preprocessing Package
import pandas as pd
from os import path


def connect_driver():
    # 드라이버 옵션
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # 드라이버 객체 호출
    return webdriver.Chrome(DRIVER_PATH, chrome_options=chrome_options)


def _get_month_range(start_dt, end_dt):
    """월 단위 시작-종료 날짜 생성함수

    Args:
        start_dt (date_str):    시작 날짜 (20111111 의 형태)
        end_dt (date_str):      종료 날짜 (20111111 의 형태)

    Returns:
        list of tuple: 월별 (시작, 종료) 리스트
    """
    base_date_range = pd.date_range(start =start_dt, end =end_dt, freq ='M')
    left_dates = [start_dt] + (base_date_range + pd.offsets.Day()).strftime('%Y%m%d').tolist()[:-1]
    right_dates = list(base_date_range.strftime('%Y%m%d')) + [end_dt]
    month_range_tuple_list = [(l, r) for l, r in zip(left_dates, right_dates) if r > l]
    return month_range_tuple_list


def _create_query(keyword, relate, pattern, start_dt, end_dt):
    """query url 생성 함수

    Args:
        keyword (str):          검색 키워드
        relate (int):           정렬방식 : 0 관련도순, 1 최신 순, 2 오래된 순
        pattern (int):          검색유형 설정: 0 전체, 1 제목
        start_dt (date str):    시작 날짜
        end_dt (date str):      종료 날짜
    Returns:
        str: query url
    """
    query_url='https://search.naver.com/search.naver?where=news&sm=tab_jum&query={}'.format(keyword)
    full_query_url = "{}&sm=tab_opt&sort={}&photo=0&field={}&reporter_article=&pd=3&ds={}&de={}".format(query_url, relate, pattern, start_dt, end_dt)
    return full_query_url


def get_query_url_list(keyword_list, relate, pattern, start_dt, end_dt):
    """ 조건에 맞는 전체 query url 생성 함수

    Args:
        keyword_list (list of str):  검색 키워드 목록    
        relate (int):           정렬방식 : 0 관련도순, 1 최신 순, 2 오래된 순
        pattern (int):          검색유형 설정: 0 전체, 1 제목
        start_dt (date str):    시작 날짜
        end_dt (date str):      종료 날짜
    Returns:
        [list of str]: query url list
    """
    month_range = _get_month_range(start_dt, end_dt)
    
    query_url_list = []
    for keyword in keyword_list:
        for dts in month_range:
            query_url = _create_query(keyword, relate, pattern, dts[0], dts[1])
            query_url_list.append(query_url)
    
    return query_url_list


def get_all_news_seed_urls(query_url_list, max_page=-1):
    """질의 쿼리(query_url)를 이용하여 질에 맞는 개별 뉴스 링크를 가지고 오는 함수

    Args:
        query_url_list (list of str): 질의 쿼리 목록
        max_page (int, optional): 최대 수집 페이지수, -1일 경우 전체 페이지 수집 Defaults to -1
    """
    
    href_list = []
    for query_url in query_url_list:
        # driver 연결
        driver = connect_driver()
        # html 정보 가져오기
        driver.get(query_url)
        
        #  max_page를 통해 최대 페이지 수를 조정(수정가능)
        i = 1
        while True:
            try :
                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
                # div tag의 class 값이 group_news인 것에서 뉴스링크만 가지고 옴
                body_tag_html = soup.find('div', {'class': 'group_news'})
                a_tags = body_tag_html.find_all(lambda tag: tag.name == 'a' and tag.get('class') == ['info'])
                if a_tags != []:
                    for a_tag in a_tags:
                        href_list.append(a_tag["href"])
                
                # 다음페이지 클릭
                btn_xpath = "//*[@id='main_pack']/div[2]/div/a[2]"
                if driver.find_element_by_xpath(btn_xpath).get_attribute('href') is not None:
                    driver.find_element_by_xpath(btn_xpath).click()
                    time.sleep(1)
                else:
                    break
                    
                if max_page != -1:
                    if i == max_page:
                        break
                    else:
                        i+=1
            except Exception as ex:
                print("에러발생", ex)
                break
            finally:
                print("{}: {}".format(query_url, len(href_list)))
        driver.quit()
        
    # 중복 제거 후 리스트화
    href_list = list(set(href_list))
    print("수집 대상 뉴스 링크 수: {}".format(len(href_list)))
    return href_list


def get_news_content(driver, url):
    def tag_type1(soup):
        papers=soup.find_all("a",{'class' : 'nclicks(atp_press)'})
        for paper in papers:
            img=paper.find("img")
            newspaper=img["title"]#발행사
        title= soup.find("h3",{'id' : 'articleTitle'}).get_text(strip=True) #제목
        day=soup.find('span', class_ = 't11').get_text(strip=True)#날짜
        day=day[0:10]
        text=soup.find('div', {'id' : 'articleBodyContents'}).get_text(strip=True)#텍스트
        return pd.DataFrame([[newspaper, day, title, text, url]], columns=["newspaper","day","title","text","link"])


    def tag_type2(soup):
        newspaper=soup.find("p",class_='source').get_text(strip=False)
        newspaper=newspaper.replace('기사제공', "")#발행사
        title=soup.find('h4',class_="title").get_text(strip=True)#제목
        day=soup.find("div",class_='info').get_text(strip=True)#날짜
        day=day[5:15]
        text=soup.find('div',class_='news_end font1 size3' ).get_text(strip=True)#텍스트
        return pd.DataFrame([[newspaper, day, title, text, url]], columns=["newspaper","day","title","text","link"])

    def tag_type3(soup):
        papers=soup.find("div",class_='press_logo')
        for paper in papers:
            img=paper.find("img")
            newspaper=img['alt']
        title=soup.find('h2',class_="end_tit").get_text(strip=True)#제목
        day=soup.find("span",class_='author').get_text(strip=True)#날짜
        day=day[4:14]
        text=soup.find('div',class_='end_body_wrp' ).get_text(strip=True)#텍스트
        return pd.DataFrame([[newspaper, day, title, text, url]], columns=["newspaper","day","title","text","link"])


    driver.get(url)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    df, error_link = None, None
    try:# 첫번째 태그 유형 실행
        df = tag_type1(soup)
        time.sleep(1)
    except AttributeError:
        try:# 두번째 태그 유형 실행
            df = tag_type2(soup)
            time.sleep(1)
        except AttributeError:
            try :# 세번째 태그 유형 실행
                df = tag_type3(soup)
                time.sleep(1)
            except Exception as ex:
                print("에러발생", ex) #세번째 태그 유형 실시후 그 외의 에러 유형은 에러리스트에 추가한다.
                error_link = url
    finally:
        return df, error_link


if __name__ == "__main__":
    
    #################
    # 크롬드라이버의 위치
    DRIVER_PATH =  './chromedriver.exe'
    
    # 저장 파일 위치
    seed_path = './seed_url.xlsx'
    data_path = './news.xlsx'
    errors_path = './errors.xlsx'
    
    # 쿼리 파라매터
    keyword_list = ['탄소 중립', '탄소 절감', '탄소 저감',  '넷 제로']
    relate, pattern = 1, 0
    start_dt = '20200101'
    end_dt = '20201231'
    
    # 본 뉴스 컨텐츠 수집시 드라이버 재 연결 기준 뉴스 수 설정
    QUIT_N = 100
    #################


    # 1. GET SEED
    all_news_seed_urls = None
    if path.exists(seed_path):
        all_news_seed_urls = pd.read_excel(seed_path, engine='openpyxl')['href_link'].tolist()
    else:
        try:
            query_url_list = get_query_url_list(keyword_list, relate, pattern, start_dt, end_dt)
            all_news_seed_urls = get_all_news_seed_urls(query_url_list, max_page=-1)
        except Exception as e:
            print(e)
        finally:
            if all_news_seed_urls is not None:
                seed_df = pd.DataFrame(all_news_seed_urls, columns=['href_link'])
                seed_df.to_excel(seed_path, sheet_name='sheet1', index=False)
            else:
                print("seed url is nothing!")

    # 2. GET CONTENTS 
    if all_news_seed_urls:
        
        # create a empty dataframe and list 
        cols = ["newspaper","day","title","text","link"]
        full_df = pd.DataFrame(columns=cols)
        error_link_list = []
        try:
            driver = connect_driver()   
            for index, url in enumerate(all_news_seed_urls):
                try:
                    df, error_link = get_news_content(driver, url)
                    if df is not None:
                        full_df = full_df.append(df)
                    if error_link is not None:
                        error_link_list.append(error_link)
                except Exception as e:
                    print(e)

                if index > 0 and index % QUIT_N == 0:
                    driver.quit()
                    driver = connect_driver()
        except Exception as e:
            print(e)
        finally:
            # QUIT DRIVER
            driver.quit()
            
            # SAVED DATA (contents, errors)
            full_df.to_excel(data_path, sheet_name='sheet1', index=False) 
            error_df = pd.DataFrame(error_link_list, columns=['error_url'])
            error_df.to_excel(errors_path, sheet_name='sheet1', index=False) 
    else:
        print("need to a seed url!")