import pandas as pd
from datetime import date, timedelta, datetime
import json
import vk_api
import random
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'rashat_musin',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 28),
    'retries': 0
}

dag = DAG(dag_id='report_vk',
          default_args=default_args,
          catchup=False,
          schedule_interval='00 14 * * *')


def send_report_to_vk():
    # Чтение данных
    path = '/mnt/c/Users/rasha/ads_data_.csv'
    ads_data = pd.read_csv(path)
    print('Данные считаны')
    # Расчет метрик
    # Просмотры
    ads_views = ads_data.query('event == "view"').groupby(['date', 'ad_id']).count().reset_index()[
        ['date', 'ad_id', 'event']]
    ads_views.columns = ['date', 'ad_id', 'views']
    # Клики
    ads_clicks = ads_data.query('event == "click"').groupby(['date', 'ad_id']).count().reset_index()[
        ['date', 'ad_id', 'event']]
    ads_clicks.columns = ['date', 'ad_id', 'clicks']
    # Объединение кликов и просмотров
    ads_merge = pd.merge(ads_views, ads_clicks, on=['date', 'ad_id'])

    # CTR
    ads_merge['ctr'] = ads_merge.clicks / ads_merge.views

    # Сумма потраченных денег
    # Так как значение всего одно можно просто умножить на просмотры
    ad_cost = ads_data.ad_cost.unique() / 1000
    ads_merge['money'] = ads_merge.views * ad_cost

    # Для постоянного использования скрипта в будущем необходимо отслеживать данные за сегодня и вчера.
    # Получить даты сегодняшнего и вчерашнего дней можно так
    # current_date = date.today()
    # yesterday_date = current_date - timedelta(1)

    # Но для данного примера используем текущие даты ниже
    current_date = '2019-04-02'
    yesterday_date = '2019-04-01'

    # Функция для получения основых метрик
    def get_metrics(df, date):
        money = float(df.query('date == @date')['money'])
        views = float(df.query('date == @date')['views'])
        clicks = float(df.query('date == @date')['clicks'])
        ctr = float(df.query('date == @date')['ctr'])
        return money, views, clicks, ctr

    # Основные метрики
    money_today, views_today, clicks_today, ctr_today = get_metrics(ads_merge, current_date)
    money_yesterday, views_yesterday, clicks_yesterday, ctr_yesterday = get_metrics(ads_merge, yesterday_date)

    # Функция для получения разницы по сравнению с прошлым днем в процентах
    def diff_from_yesterday(value_today, value_yesterday):
        return round((value_today - value_yesterday) / value_yesterday * 100)

    # Проценты разницы со вчерашним днем
    diff_money = diff_from_yesterday(money_today, money_yesterday)
    diff_views = diff_from_yesterday(views_today, views_yesterday)
    diff_clicks = diff_from_yesterday(clicks_today, clicks_yesterday)
    diff_ctr = diff_from_yesterday(ctr_today, ctr_yesterday)
    print('Метрики рассчитаны')

    # Создание отчета
    message = f'''Отчет по объявлению "121288" за {current_date}:
    Траты: {money_today} рублей ({diff_money}%)
    Показы: {views_today} ({diff_views}%)
    Клики: {clicks_today} ({diff_clicks}%)
    CTR: {ctr_today} ({diff_ctr}%)'''
    print('Отчет создан')

    # Сохраняем отчет
    with open(f'report_{current_date}.txt', 'w', encoding='UTF-8') as file:
        file.write(message)
    print('Файл сохранен')

    # Отправляем отчет в личные сообщения в VK
    with open('/mnt/c/Users/rasha//vk_token.json') as src:
        credentials = json.load(src)
    token = credentials['token']
    my_id = 144925167
    vk_session = vk_api.VkApi(token=token)
    vk = vk_session.get_api()
    vk.messages.send(user_id=my_id,
                     message=message,
                     random_id=random.randint(1, 2 ** 31))
    print('Отчет отправлен')


t1 = PythonOperator(task_id='ads_report',
                    python_callable=send_report_to_vk,
                    dag=dag)


