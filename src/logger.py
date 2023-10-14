import sqlite3

import pandas as pd
import socket
from datetime import datetime, timedelta
import re
from collections import defaultdict
import threading
import os

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

        self.udp_socket = udp_socket

        self.current_session = datetime.now().date()
        self.last_write = datetime.now()
        self.listening = False

        self.finished = pd.DataFrame(data=[], columns=['ID', 'Session', 'A', 'B', 'C', 'V', 'H', 'Notes'])
        self.ongoing = defaultdict(lambda: pd.DataFrame(
            data=[[None, self.current_session, None, None, None, None, '']],
            columns=['ID', 'Session', 'A', 'B', 'C', 'V', 'H', 'Notes']))

        self.buffer_size = 1024
        if udp_socket is not None:
            if type(udp_socket) in [list, tuple] and len(udp_socket) == 2:
                self.udp_host, self.udp_port = udp_socket
            elif type(udp_socket) == socket.socket:
                socket_info = re.match(r"[\w\W\s]*laddr=\('(\d+.\d+.\d+.\d+)', (\d+)\)>", str(udp_socket))
                if socket_info is not None:
                    self.udp_host = socket_info[1]
                    self.udp_port = socket_info[2]
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

                self.update_session()
                self.log_to_text_files(time, data)
                self.log_to_database(time, data)
                self.update_data(time, data)
            except TimeoutError:
                # Designed to happen so stop event can trigger thread end
                pass
            except Exception as e:
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
        self.udp_socket.bind((udp_host, udp_port))
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
                'udp_log_ALL.txt'
                f'udp_log_{self.current_session:%m.%d.%y}.txt'
            ]
        return self.log_files

    def setup_database(self, database):
        """
        Sets up database connection, as well as initial database creation if database file does not exist

        :param database: Database file name
        :type database: string
        :return:
        """
        database_exists = os.path.exists(database)

        # Generates folder structure
        folders = database.split('/')
        for i in range(len(folders)-1):
            temp = '/'.join(folders[:i+1])
            if not os.path.exists(temp):
                os.mkdir(temp)

        # Connecting to database
        self.database_conn = sqlite3.connect(database)
        c = self.database_conn.cursor()
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

    def update_session(self):
        """
        Updates session info and log files if:
            1. Current session isn't today
            2. More than one hour since last write has passed
        :return:
        """
        if (datetime.now().date() != self.current_session and
                (datetime.now() - self.last_write) / timedelta(hours=1) > 1):
            # Update any log files
            for i in range(len(self.log_files)):
                self.log_files[i] = self.log_files[i].replace(
                    f'{self.current_session:%m.%d.%y}',
                    f'{datetime.now():%m.%d.%y}'
                )
            self.current_session = datetime.now().date()
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
        cursor = self.database_conn.cursor()
        cursor.execute("INSERT INTO Logs VALUES(?,?,?);", (time, self.current_session, message))
        self.database_conn.commit()

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
        curr[-1] = curr[-1][:-2]
        curr = tuple(curr)
        del self.ongoing[id]

        c = self.database_conn.cursor()
        c.execute("INSERT INTO BadgeScans VALUES(?,?,?,?,?,?,?,?)", curr)
        self.database_conn.commit()
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