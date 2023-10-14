from src.logger import Logger
from time import sleep

if __name__ == '__main__':
    udp_socket = ('localhost', 50001)
    l = Logger(udp_socket=udp_socket)
    l.start()
    sleep(1)

    close_logger = False
    while not close_logger:
        instruction = ''
        try:
            instruction = input("Type exit to end logger.\n")
        except Exception as e:
            print(f'Exception occurred: {e}')
            close_logger = True

        if instruction.strip().upper() == 'EXIT':
            close_logger = True
        elif instruction != '':
            print(f'Did not understand input: {instruction}')

    print('Exiting')
    l.stop()
