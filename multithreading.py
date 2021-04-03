import os
import requests
import random
import urllib
import re
from bs4 import BeautifulSoup
from queue import Queue
import threading
import time


'''
Утилита, которая в потоках рекурсивно обходит ссылки сайта и скачивает с этих страниц картинки в папку.
Начальной точкой обхода является сайт: http://bgu.ru/ .
Вся работа распределена на несколько потоков:
1. Один поток выводит на экран информацию о работе других потоков
2. По одному потоку проверки на скачивание картинки или сканирование ссылки ранее
3. Группа потоков (в данном проекте 3) для сканирования HTML-страниц из очереди и перехода по ссылкам рекурсивно
4. Группа потоков (в данном проекте 5) для скачивания картинок в папку из очереди
5. Один поток для отслеживания работы других поток. Он управляет установкой значений мьютексов mutex_warning_exit и mutex_exit, а также побуждает другие поток завершиться.
'''

useragents = [
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
    ]

headers = {
    'User-Agent': random.choice(useragents)
}

# Блокировки для общих данных
lock_links = threading.Lock()
lock_imgs = threading.Lock()

# Повторяющиеся ссылки и картинки
passed_links = ['http://bgu.ru/']
passed_imgs = []
my_path = 'Images/'

# Очереди
# Очереди сканируемых ссылок и скачиваемых картинок
q_imgs = Queue()
q_links = Queue()
q_links.put('http://bgu.ru/')
# Очередь сообщений в консоль
q_msgs = Queue()
q_msgs.put('Основной поток запущен')
# Очереди проверяемых на повтор ссылок и картинок
q_checked_imgs = Queue()
q_checked_links = Queue()

# Переменные для управления завершением программы
mutex_exit = threading.Semaphore()
mutex_warning_exit = threading.Semaphore()


# Проверка глобального мьютекса для выхода потоков
def check_mutex(num):
    if mutex_exit.__reduce__()[2].get('_value') == 0:
        print('Поток закончил работу -- ' + str(num))
        # Завершение потока
        exit()


# Проверка глобального предупредительного мьютекса для выхода потоков
def check_warning_mutex():
    # Предупредительный флаг не опущен
    if mutex_warning_exit.acquire(blocking=False) is False:
        # Опускаем предупредительный флаг
        mutex_warning_exit.release()


# Проверка картинки
def check_img(barrier, num):
    q_msgs.put('Запуск {} потока проверки картинок'.format(num))
    barrier.wait()
    while True:
        # Очередь не пустая
        if not q_checked_imgs.empty():
            # Вызов проверки предупредительного мьютекса
            check_warning_mutex()
            # Блокирующаяся очередь, во избежании коллизий
            img = q_checked_imgs.get(block=True)

            if img not in passed_imgs:
                passed_imgs.append(str(img))
                if len(re.findall(r'((jpg)|(jpeg)|(png))$', str(img))) != 0:
                    # Добавление в очередь картинок
                    q_imgs.put(img)
        check_mutex(num)


# Проверка ссылок
def check_link(barrier, num):
    q_msgs.put('Запуск {} потока проверки ссылок'.format(num))
    barrier.wait()
    while True:
        # Очередь не пустая
        if not q_checked_links.empty():
            # Вызов проверки предупредительного мьютекса
            check_warning_mutex()
            # Блокирующаяся очередь, во избежании коллизий
            link = q_checked_links.get(block=True)
            if link not in passed_links:
                passed_links.append(str(link))
                if len(re.findall(r'((mailto:))|(tel:)', str(link))) == 0:
                    # Добавление в очередь ссылок
                    q_links.put(link)
        check_mutex(num)


# Скачивание картинок, картинки берутся из очереди q_imgs
def save_img(barrier, num):
    q_msgs.put('Запуск {} потока сканирования картинок'.format(num))
    barrier.wait()
    while True:
        img=''
        # Очередь не пустая
        if not q_imgs.empty():
            # Вызов проверки предупредительного мьютекса
            check_warning_mutex()
        try:
            # По факту, блокировка не нужна, потому что взятие сообщения из очереди и так атомарная процедура
            with lock_imgs:
                img = str(q_imgs.get(block=False))

            if img != '':
                # Вставка сообщения в очередь
                q_msgs.put('Загружается картинка: ' + img)
                try:
                    urllib.request.urlretrieve(img, os.path.join(my_path, os.path.basename(img)))
                except Exception:
                    print('Не смог загрузить картинку ' + str(img))
        except Exception:
            pass
        check_mutex(num)


# Поиск ссылок и картинок, заполнение очередей для проверки ссылок и картинок
def collect_links(barrier, num):
    q_msgs.put('Запуск {} потока сканирования ссылок'.format(num))
    barrier.wait()
    while True:
        if not q_links.empty():
            check_warning_mutex()
        soup = BeautifulSoup()
        href = ''
        try:
            # Блокировка на доступ к общим данным разными потоками
            with lock_links:
                link = q_links.get(block=False)
            soup = BeautifulSoup(requests.get(url=link, headers=headers).text, 'html.parser')
        except Exception:
            pass
        # Получение ссылок и картинок на этой странице
        links = soup.findAll("a")
        imgs = soup.findAll("img")

        for img in imgs:
            img = img['src']
            if not re.match(r'^(http:)', str(img)):
                # todo Если ссылка указывает на файл .aspx работает неправильно
                img = link + '/' + str(img)
            # Добавление в очередь картинок на проверку
            q_checked_imgs.put(img)

        for link in links:
            try:
                href = link.get('href')
            except:
                pass
            if not re.match(r'^(http:)', str(href)):
                href = 'http://bgu.ru/' + str(href)
            if re.match(r'^(http://bgu.ru/)', str(href)):
                # Добавление в очередь ссылок на проверку
                q_checked_links.put(href)
        check_mutex(num)


# Печать статистики
def print_stat(barrier, num):
    q_msgs.put('Запуск {} потока печати статистики'.format(num))
    barrier.wait()
    while True:
        # Очередь не пустая
        if not q_msgs.empty():
            # Вызов проверки предупредительного мьютекса
            check_warning_mutex()
            # Печать сообщения из очереди
            print(q_msgs.get())
        check_mutex(num)


# Проверка условий выхода из программы и установка глобального мьютекса
def exit_program(barrier, num):
    q_msgs.put('Запуск {} потока выхода из программы'.format(num))
    barrier.wait()
    while True:
        # Очереди пустые
        if q_imgs.empty() and q_links.empty() and q_msgs.empty():

            # Поднимаем предупредительный флажок - мьютекс (установка 0)
            mutex_warning_exit.acquire(blocking=False)
            # Даем время другим потокам опустить флажок
            time.sleep(5)
            # Если никто не опустил предупредительный флажок, то поднимамем основной флаг выхода всех потоков
            if mutex_warning_exit.acquire(blocking=False) is False:
                # Установка 0 на основной мьютекс
                mutex_exit.acquire()
                print('Поток закончил работу -- ' + str(num))
                exit()
        time.sleep(5)


# Входная функция
def main():
    q_num = Queue()
    for i in range(12):
        q_num.put(i)

    barrier = threading.Barrier(4)
    # Поток печати статистики
    th_print = threading.Thread(target=print_stat, args=(barrier, q_num.get(block=False)))
    th_print.start()

    # Поток проверки ссылок
    th_check_link = threading.Thread(target=check_link, args=(barrier, q_num.get(block=False)))
    th_check_link.start()

    # Поток проверки картинок
    th_check_img = threading.Thread(target=check_img, args=(barrier, q_num.get(block=False)))
    th_check_img.start()

    # Поток выхода из программы
    th_exit = threading.Thread(target=exit_program, args=(barrier, q_num.get(block=False)))
    th_exit.start()

    # Потоки сканирования ссылок
    barrier_collect_links = threading.Barrier(3)
    for i in range(3):
        th_collect_links = threading.Thread(target=collect_links, args=(barrier_collect_links, q_num.get(block=False)))
        th_collect_links.start()

    # Потоки закачки картинок
    barrier_save_img = threading.Barrier(5)
    for i in range(5):
        th_save_img = threading.Thread(target=save_img, args=(barrier_save_img, q_num.get(block=False)))
        th_save_img.start()


main()
