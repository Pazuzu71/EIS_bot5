import logging


def create_logger(module_name):
    # Настраиваем базовую конфигурацию логирования
    logging.basicConfig(
        format='[%(asctime)s] #%(levelname)-8s %(filename)s: %(lineno)d - %(name)s:%(funcName)s - %(message)s',
        level=logging.INFO,
    )
    # Инициализируем логгер модуля
    logger = logging.getLogger(module_name)
    # Устанавливаем логгеру уровень `DEBUG`
    logger.setLevel(logging.DEBUG)
    # Инициализируем хэндлер, который будет писать логи в файл `error.log`
    error_file = logging.FileHandler('error.log', 'a', encoding='utf-8')
    # Устанавливаем хэндлеру уровень `DEBUG`
    error_file.setLevel(logging.DEBUG)
    # Инициализируем форматтер
    formatter_1 = logging.Formatter(
        fmt='[%(asctime)s] #%(levelname)-8s %(filename)s:'
            '%(lineno)d - %(name)s:%(funcName)s - %(message)s'
    )
    # Определяем форматирование логов в хэндлере
    error_file.setFormatter(formatter_1)
    # Добавляем хэндлер в логгер
    logger.addHandler(error_file)

    return logger
