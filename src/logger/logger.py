import pandas as pd
import socket
from datetime import datetime
import re
from collections import defaultdict

# TODO: Test logging and data updating functionality
# TODO: implement analysis functionality

class AutoUpdater:

    pass

class Logger:

    def __init__(self, log_file=None, data_file=None, udp_socket=None, auto_save=True):
        """
        Logger object to handle writing and interpreting udp messages

        :param log_file: Name of file to output log messages
        :type log_file: string
        :param data_file: Name of file to output formatted data to
        :type data_file: string
        :param udp_socket: udp_socket connection
        :type udp_socket:
        :param auto_save: Option to automatically save data when logger is deleted
        :type auto_save: bool
        :return: None
        """
        curr_time = datetime.now()
        self.log_file = log_file if log_file is not None else f'udp_log_{curr_time:%m.%d.%y}.txt'
        self.data_file = data_file if data_file is not None else f'checkpoint_data_{curr_time:%m.%d.%y}.csv'
        self.udp_socket = udp_socket
        self.auto_save = auto_save
        self.finished = pd.DataFrame(data=[], columns=['ID', 'A', 'B', 'C', 'V', 'H', 'Notes'])
        self.ongoing = defaultdict(lambda: pd.DataFrame(
            data=[[None, None, None, None, None, None, []]],
            columns=['ID', 'A', 'B', 'C', 'V', 'H', 'Notes']))
        self.listening = False

        if udp_socket is not None:
            socket_info = re.match(r"[\w\W\s]*laddr=\('(\d+.\d+.\d+.\d+)', (\d+)\)>", str(udp_socket))
            if socket_info is not None:
                self.udp_host = socket_info[1]
                self.udp_port = socket_info[2]
            else:
                self.udp_host = 'Unknown'
                self.udp_port = 'Unknown'

    def __del__(self):
        """
        Closes udp socket and dumps data into file

        :return: None
        """
        pass
        # self.close_udp()
        # if self.auto_save:
        #     self.dump_data()

    def listen(self):
        """
        Sets logger to listen to udp socket, recording messages, until Keyboard Interrupt or other error

        :return: None
        """
        # TODO Implement threading to allow multiple loggers to run concurrently
        # TODO Create better exiting criteria

        # If no udp socket, uses default
        if self.udp_socket is None:
            self.create_udp_socket()
        print(f'Listening for UDP messages on {self.udp_host}:{self.udp_port}')
        while True:
            try:
                data, addr = self.udp_socket.recvfrom(self.buffer_size)
                try:
                    data = data.decode('ascii')
                except:
                    pass
                print(f'Received message from {addr}: {data}')

                time = datetime.now()
                self.log_message(time, data)
                self.update_data(time, data)
            except KeyboardInterrupt:
                print('Ending logging')
                break
            except Exception as e:
                print(f'Error occurred: {e}')
        self.close_udp()
        if self.auto_save:
            self.dump_data()

    def create_udp_socket(self, udp_host='0.0.0.0', udp_port=12345, buffer_size=1024):
        """
        Creates udp connection for the logger object. Returns the socket and sets logger property

        :param udp_host: UDP host IP, default to 0.0.0.0
        :param udp_port: UDP port, default to 12345
        :param buffer_size: UDP buffer size for receiving, default to 1024
        :return: udp socket
        """
        self.udp_host = udp_host
        self.udp_port = udp_port
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((udp_host, udp_port))
        self.buffer_size = buffer_size
        return self.udp_socket

    def log_message(self, time, message):
        """
        Logs message and time to log file

        :param time: Time of the message
        :param message: Message received from udp socket
        :return: None
        """
        with open(self.log_file, 'a+') as file:
            file.write(f'{time:%Y-%m-%d %H:%M:%S} - {message}\n')

    def update_data(self, time, message):
        """
        Updates the structured data, moving things from ongoing to finished if at haunt

        :param time: Time of the message
        :param message: Message received from udp socket
        :return: None
        """

        # TODO: if id is in ongoing at A, finalize

        data = re.match(r'([ABCVH]) ([\w]{8})$', message)
        if data is None:
            # Not a station card combo. Adds data to notes
            for id in self.ongoing:
                self.ongoing[id].iloc[0]['Notes'].append(message.strip())
        else:
            # Adds time data to ongoing entry
            station, id = data[1], data[2]
            self.ongoing[id].iloc[0][station] = time

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
        curr = self.ongoing[id]
        del self.ongoing[id]
        curr.iloc[0]['ID'] = id
        self.finished.loc[len(self.finished)] = curr.values[0]

    def close_udp(self):
        """
        Closes udp connection

        :return: None
        """
        try:
            self.udp_socket.close()
        except:
            print('Could not close udp socket')

    def dump_data(self):
        """
        Writes any data currently in ongoing and then saves to data file

        :return: None
        """
        ids = list(self.ongoing.keys())
        for id in ids:
            self.finalize(id)
        self.finished.to_csv(self.data_file, index=False)

if __name__ == '__main__':
    logger = Logger()
    logger.listen()