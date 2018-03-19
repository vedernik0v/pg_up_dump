#!/usr/bin/env python2
# -*- coding: utf-8 -*-
# encoding: utf-8

import mimetypes
import optparse
import os
import subprocess
import sys

from inspect import currentframe, getframeinfo
frameinfo = getframeinfo(currentframe())


class DbOptions(object):
    '''
    Абстрацция опций подключения к БД
    '''
    
    def __init__(self, dbname=None, user=None, host='localhost', port=None, password=None):
        self.dbname = dbname
        self.user = user
        self.host = host
        self.port = port
        self.password = password


    @property
    def options(self):
        '''
        Возвращает список опций
        '''

        opts = []
        if self.dbname != None:
            opts.append('-d%s' % self.dbname)
        if self.user != None:
            opts.append('-U%s' % self.user)
        if self.host != None:
            opts.append('-h %s' % self.host)
        if self.port != None:
            opts.append('-p %s' % self.port )
        if self.password != None:
            opts.append('-W %s' % self.password )

        return opts


    @property
    def options_string(self):
        '''
        Возвращает строку с параметрами подключения
        
        [-d dbname] [-U user] [-h hostname] [-p port] [-W password]
        '''
        opts = self.options
        return ' '.join(opts)


    @property
    def connection_string(self):
        '''
        Возвращает строку подключения
        
        postgresql://[user[:password]@][netloc][:port][/dbname]
        '''
        connection_string = 'postgres//'
        if self.user != None:
            connection_string += self.user
        if self.password != None:
            connection_string += ':' + self.password
        connection_string += '@'
        if self.host != None:
            connection_string += self.host
        if self.port != None:
            connection_string += ':' + self.port
        if self.dbname != None:
            connection_string += '/' + self.dbname
        
        return connection_string


    def __str__(self):
        return self.connection_string        


class PgSQL(object):
    '''
    Абстракция взаимодействия с PostgreSQL
    '''
    def __init__(self, connect_options):
        self.connect_optopns = connect_options
        self.__prefix_cmd = "psql " + connect_options
 
    def test_connect(self):
        ''' Пробное подключение к БД '''
        cmd = self.__prefix_cmd + " -c '\q'"
        return os.system(cmd) == 0

    def clear_db(self):
        ''' Чистка БД (удаление таблиц) '''
        sql_file = '/var/www/vega/vedernik.ru/mini-shd/clear_db.sql'
        cmd = "%s -f %s" % (self.__prefix_cmd, sql_file)
        print cmd
        return os.system(cmd)

    def up_dump(self, sql_file):
        ''' Загрузка dump-файла '''
        cmd = "%s -f %s" % (self.__prefix_cmd, sql_file)
        print cmd
        return os.system(cmd)


class DumpFile(object):
    '''
    Абстракция файла дампа БД
    '''

    def __init__(self, filename=None, search_dir=None):
        if filename != None:
            self.filename = os.path.abspath(filename)
            self.validate_filename()
        elif  search_dir != None:
            self.search_dir = os.path.abspath(search_dir)
            self.validate_search_dir()
            self.file_search()
        else:
            sys.exit('Необходимо указать одну из опций -D, -F !')


    def validate_filename(self):
        ''' Проверка  dump-файла '''
        is_exists = os.path.exists(self.filename)
        if not is_exists:
            sys.exit("Файл %s не найден!" % self.filename)
        
        is_file = os.path.isfile(self.filename)
        if not is_file:
            sys.exit("%s не является файлом!" % self.filename)
        
        mimetype_filename = mimetypes.guess_type(self.filename)
        if mimetype_filename[0] != 'application/x-sql':
            sys.exit('Файл %s (%s) не является дампом БД!' % (self.filename, mimetype_filename[0]))


    def validate_search_dir(self):
        ''' Проверка директории поиска dump-файла '''
        is_exists = os.path.exists(self.search_dir)
        if not is_exists:
            sys.exit("Директория %s не найдена!" % self.search_dir)
        
        is_dir = os.path.isdir(self.search_dir)
        if not is_dir:
            sys.exit('%s не является директорией' % self.search_dir)


    def mv2tmp(self):
        ''' Пемещает dump-файл в директорию /tmp/ '''
        os.rename(self.filename, "/tmp/%s"%os.path.basename(self.filename))


    def file_search(self):
        sql_files = []
        files = os.listdir(self.search_dir)
        files = [os.path.join(self.search_dir, f) for f in files]
        files = [f for f in files if os.path.isfile(f)]
        for f in files:
            mimetype_filename = mimetypes.guess_type(f)
            if mimetype_filename[0] == 'application/x-sql':
                sql_files.append(f)
                print "%s - %s"%(os.path.getctime(f), f)
        if len(sql_files):
            self.filename = max(sql_files, key=os.path.getctime)


class MiniShdService(object):
    '''
    Абстракция службы хранения данных
    '''

    def __init__(self):
        self.name = 'mini-shd'
        self.__cmd_list =  ['start', 'stop']

    def __run_cmd(self, cmd):
        try:
            self.__cmd_list.index(cmd)
        except ValueError:
            sys.exit('ОШИБКА! Была попытка выполнить недрпустимая команду.')
 
        cmd = ' '.join(['systemctl', cmd,  self.name])

        return os.system(cmd)

    def start(self):
        return self.__run_cmd('start')

    def stop(self):
        return self.__run_cmd('stop')

    @property
    def is_active(self):
        exit_status = os.system("systemctl is-active %s" % self.name)
        return exit_status == 0


def get_options():
    '''
    Возвращает опции, указанные в командной строке
    '''
    parser = optparse.OptionParser()
    parser.add_option("-d", "--dbname", dest="dbname", help="name of database")
    parser.add_option("-U", "--user", dest="dbuser", help="user of database")
    parser.add_option("-H", "--host", dest="dbhost", help="hostname of database")
    parser.add_option("-p", "--port", dest="dbport", help="port of database")
    parser.add_option("-P", "--password", dest="dbpass", help="user password of database")
    parser.add_option("-F", "--dump-file", dest="dump_file", help="database dump filename")
    parser.add_option("-D", "--dump-dir", dest="dump_dir", help="directory for search database dump filename")
 
    opts, args = parser.parse_args()

    return opts, args


def main():
    opts, args = get_options()
    
    db_opts = DbOptions(opts.dbname, opts.dbuser, host=opts.dbhost, port=opts.dbport, password=opts.dbpass)
    
    psql = PgSQL(db_opts.options_string)
    if not psql.test_connect():
        sys.exit('Не удалось подключиться к БД')

    dump = DumpFile(filename=opts.dump_file, search_dir=opts.dump_dir)
    try:
        print "Выбран dump-файл '%s'"%dump.filename
    except AttributeError:
        sys.exit("Dump-файл не был найден.")

    mini_shd = MiniShdService()

    if not mini_shd.stop() == 0:
        sys.exit("ОШИБКА: Не удалось остановить службу %s" % mini_shd.name )
    
    ec = psql.clear_db()
    print ec
    #if not ec == 0:
    #    sys.exit("ОШИБКА: Не удалось почистить БД")

    if psql.up_dump(dump.filename) == 0:
        dump.mv2tmp()
    else:
        sys.exit("ОШИБКА: Не удалось обновить БД")

    if not mini_shd.start() == 0:
        sys.exit("ОШИБКА: Не удалось запустить службу ''" % mini_shd.name)


if __name__ == "__main__":
    main()

