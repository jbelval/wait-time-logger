import sqlite3
import pandas as pd
import socket
from datetime import datetime, timedelta
import re
from collections import defaultdict
import threading
import os
import pathlib

# TODO: Test logging and data updating functionality
# TODO: implement analysis functionality

class Logger(threading.Thread):

    def __init__(self, log_files=None, database='./data/scans.db', udp_socket=None, auto_save=True):
        """
        Logger object to handle writing and interpreting udp messages

        :param log_files: Name of file to output log messages
        :type log_files: list of string
        :param database: Name of database to output formatted data to
        :type database: string
        :param udp_socket: udp_socket connection or ip and port
        :type udp_socket:
        :param auto_save: Option to automatically save data when logger is deleted
        :type auto_save: bool
        :return: None
        """

        super().__init__()

        self.log_files = log_files
        self.setup_database(database)
        self.auto_save = auto_save

        self.last_write = datetime.now()
        self.current_session = self.last_write.date()
        self.listening = False
        self._stop_event = None

        self.finished = pd.DataFrame(data=[], columns=['ID', 'Session', 'A', 'B', 'C', 'V', 'H', 'Notes'])
        self.ongoing = defaultdict(lambda: pd.DataFrame(
            data=[[None, self.current_session, None, None, None, None, None, '']],
            columns=['ID', 'Session', 'A', 'B', 'C', 'V', 'H', 'Notes']))

        self.buffer_size = 1024
        self.udp_socket = None
        if type(udp_socket) in [list, tuple] and len(udp_socket) == 2:
            self.udp_host, self.udp_port = udp_socket
            self.create_udp_socket(self.udp_host, self.udp_port)
        elif type(udp_socket) == socket.socket:
            self.udp_socket = udp_socket
            socket_info = re.match(r"[\w\W\s]*laddr=\('(\d+.\d+.\d+.\d+)', (\d+)\)>", str(udp_socket))
            if socket_info is not None:
                self.udp_host = socket_info[1]
                self.udp_port = socket_info[2]
        elif udp_socket is None:
            self.udp_host = None
            self.udp_port = None
        else:
            print('Could not parse udp socket.')
            self.udp_host = None
            self.udp_port = None

    def __del__(self):
        """
        Closes udp socket and dumps data into file

        :return: None
        """
        pass
        # self.close_udp()
        # if self.auto_save:
        #     self.dump_data()

    def run(self):
        """
        Sets logger to listen to udp socket, recording messages, until Keyboard Interrupt or other error

        :return: None
        """
        if self.listening:
            print(f'Already listening on {self.udp_host}:{self.udp_port}')
            return

        # If no udp socket, uses default
        if self.udp_socket is None:
            self.create_udp_socket()
        print(f'Listening for UDP messages on {self.udp_host}:{self.udp_port}')

        self._stop_event = threading.Event()
        self.listening = True
        while not self._stop_event.is_set():
            try:
                data, addr = self.udp_socket.recvfrom(self.buffer_size)
                data = data.decode('ascii')
                time = datetime.now()
                print(f'{time:%Y-%m-%d %H:%M:%S} - Received message from {addr}: {data}')

                self.update_session(time)
                self.log_to_text_files(time, data)
                self.log_to_database(time, data)
                self.update_data(time, data)
            except TimeoutError:
                # Designed to happen so stop event can trigger thread end
                pass
            except Exception as e:
                raise e.with_traceback()
                print(f'Error occurred: {e}')
                self._stop_event.set()

        self.close_udp()
        if self.auto_save:
            print('Saving data...')
            self.dump_data()

        self.listening = False
        print(f'No longer listening for UDP messages on {self.udp_host}:{self.udp_port}')

    def stop(self):
        if self.listening:
            self._stop_event.set()
        else:
            print('Not currently listening.')

    def create_udp_socket(self, udp_host='0.0.0.0', udp_port=12345, overwrite=False):
        """
        Creates udp connection for the logger object. Returns the socket and sets logger property

        :param udp_host: UDP host IP, default to 0.0.0.0
        :param udp_port: UDP port, default to 12345
        :param overwrite: Overwrite current udp host and port info
        :return: udp socket
        """
        if overwrite or self.udp_host is None:
            self.udp_host = udp_host
        if overwrite or self.udp_port is None:
            self.udp_port = udp_port
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.udp_host, self.udp_port))
        self.udp_socket.settimeout(2)
        return self.udp_socket

    def close_udp(self):
        """
        Closes udp connection

        :return: None
        """
        try:
            self.udp_socket.close()
        except:
            print('Could not close udp socket')

    def get_text_log_files(self):
        """
        Creates default ALL and Current session log files if nothing

        :return:
        """
        if self.log_files is None:
            self.log_files = [
                './data/udp_log_ALL.txt',
                f'./data/udp_log_{self.current_session:%m.%d.%y}.txt'
            ]
            if not os.path.exists('./data'):
                os.mkdir('./data')
        return self.log_files

    def get_database_connection(self):
        """
        Gets connection to listener's database

        :return: database connection
        :rtype: sqlite3.Connection
        """
        conn = sqlite3.connect(self.database)
        conn.cursor().execute("PRAGMA jojurnal_mode=wal")
        return conn

    def setup_database(self, database):
        """
        Sets up database if database file does not exist

        :param database: Database file name
        :type database: string
        :return:
        """

        database_exists = os.path.exists(database)

        # Generates folder structure
        folder = '/'.join(database.split('/')[:-1])
        if not os.path.exists(folder):
            pathlib.Path(folder).mkdir(parents=True)

        # Connecting to database
        self.database = database
        conn = sqlite3.connect(database)
        c = conn.cursor()
        c.execute("PRAGMA journal_mode=wal")

        # If database did not exist, creates structure of tables
        if not database_exists:
            c.execute("""
                CREATE TABLE Logs(
                    time TEXT,
                    session TEXT,
                    message TEXT
                );
            """)
            c.execute("""
                CREATE TABLE BadgeScans(
                    id TEXT,
                    session TEXT,
                    a_time TEXT,
                    b_time TEXT,
                    c_time TEXT,
                    v_time TEXT,
                    h_time TEXT,
                    notes TEXT
                );
            """)
            conn.commit()

    def update_session(self, time):
        """
        Updates session info and log files if:
            1. Current session isn't today
            2. More than one hour since last write has passed
        :param time: Time of current write
        :return:
        """
        if (time.date() != self.current_session and
                (time - self.last_write) / timedelta(hours=1) > 1):
            print('Updating session and log files.')
            # Update any log files\
            log_files = self.get_text_log_files()
            for i in range(len(log_files)):
                log_files[i] = log_files[i].replace(
                    f'{self.current_session:%m.%d.%y}',
                    f'{time:%m.%d.%y}'
                )
            self.current_session = time.date()

    def log_to_text_files(self, time, message):
        """
        Logs message and time to text log files

        :param time: Time of the message
        :type time: datetime formatted as YYYY-MM-DD HH-MM-SS
        :param message: Message received from udp socket
        :type message: string
        :return: None
        """
        for file_name in self.get_text_log_files():
            with open(file_name, 'a+') as file:
                file.write(f'{time:%Y-%m-%d %H:%M:%S} - {message}\n')

    def log_to_database(self, time, message):
        """
        Logs message and time to database Logs table

        :param time: Time of the message
        :type time: datetime formatted as YYYY-MM-DD HH-MM-SS
        :param message: Message received from udp socket
        :type message: string
        :return: None
        """
        conn = self.get_database_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO Logs VALUES(?,?,?);", (time, self.current_session, message))
        conn.commit()

    def update_data(self, time, message):
        """
        Updates the structured data, moving things from ongoing to finished if at haunt

        :param time: Time of the message
        :param message: Message received from udp socket
        :return: None
        """

        data = re.match(r'([ABCVH]) ([\w]{8})$', message)
        if data is None:
            # Not a station card combo. Adds data to notes
            for id in self.ongoing:
                self.ongoing[id].loc[0, 'Notes'] += f'{message.strip()}, '
        else:
            station, id = data[1], data[2]

            # Finalizes anything prior to starting at first station
            if station == 'A' and id in self.ongoing:
                self.finalize(id)

            # Updates station time
            self.ongoing[id].loc[0, station] = time

            # If last station has been reached, adds to final data and removes from ongoing
            if station == 'H':
                self.finalize(id)

    def receive(self, message):
        """
        Logs and adds to data message
        Useful for testing without a udp connection

        :param message: Message to be logged and added to data
        :type message: string
        :return:
        """
        time = datetime.now()
        self.log_message(time, message)
        self.update_data(time, message)

    def add_from_text_log(self, log_file):
        """
        Reads in log_file as if sent by udp socket

        :param log_file: name of log file
        :type log_file: string
        :return: None
        """
        self.last_write = datetime(1900, 1, 1)
        self.current_session = self.last_write.date()
        self.log_files = None
        with open(log_file, 'r') as f:
            logs = f.readlines()
        for l in logs:
            l = l.strip()
            try:
                time, data = l.split(' - ')
                time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')

                self.update_session(time)
                self.log_to_text_files(time, data)
                self.log_to_database(time, data)
                self.update_data(time, data)
            except:
                print(f'Could not parse "{l}"')
        print('Saving data...')
        self.dump_data()

    def finalize(self, id):
        """
        Takes id's entry from ongoing and adds it to finished

        :param id: Card id
        :return: None
        """
        if id not in self.ongoing:
            raise UserWarning('Trying to finalize id not currently in ongoing.')
        curr = self.ongoing[id]
        curr = curr.iloc[0].values
        curr[0] = id
        curr[-1] = curr[-1][:-2]    # Removes ', ' from end of Notes
        curr = tuple(curr)
        del self.ongoing[id]

        conn = self.get_database_connection()
        c = conn.cursor()
        c.execute("INSERT INTO BadgeScans VALUES(?,?,?,?,?,?,?,?)", curr)
        conn.commit()
        # self.finished.loc[len(self.finished)] = curr.values[0]

    def dump_data(self):
        """
        Writes any data currently in ongoing and then saves to data file

        :return: None
        """
        ids = list(self.ongoing.keys())
        for id in ids:
            self.finalize(id)

if __name__ == '__main__':
    logger = Logger()
    logger.listen()