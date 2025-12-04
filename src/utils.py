import pandas as pd
import os
from pathlib import Path
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataSplitter:
    """
    Utility class to split large bike dataset into monthly CSV files.
    Complies with SOLID (Single Responsibility Principle).
    """

    def __init__(self, input_file: str, output_dir: str):
        """
        :param input_file: Path to the huge raw csv file.
        :param output_dir: Directory where monthly files will be saved.
        """
        self.input_file = input_file
        self.output_dir = output_dir

    def process(self, chunk_size: int = 1000000):
        """
        Reads input file in chunks and saves data grouped by month.
        """
        if not os.path.exists(self.input_file):
            logger.error(f"File not found: {self.input_file}")
            return

        # Создаем папку data/monthly, если нет
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting processing {self.input_file}...")

        try:
            # Читаем частями, чтобы не забить память (10 млн строк это много)
            for i, chunk in enumerate(pd.read_csv(self.input_file, chunksize=chunk_size)):
                self._process_chunk(chunk)
                logger.info(f"Processed chunk {i + 1}")

        except Exception as e:
            logger.error(f"Error during processing: {e}")

    def _process_chunk(self, chunk: pd.DataFrame):
        """
        Helper method to process a single chunk.
        """
        # Преобразуем колонку времени (предполагаем название Departure)
        # В датасете Хельсинки колонки обычно: Departure, Return, etc.
        # Проверь точное название колонок в твоем CSV!
        if 'Departure' in chunk.columns:
            date_col = 'Departure'
        elif 'departure_time' in chunk.columns:
            date_col = 'departure_time'
        else:
            # Fallback или ошибка, берем первую колонку с датой
            date_col = chunk.columns[0]

        chunk[date_col] = pd.to_datetime(chunk[date_col], errors='coerce')

        # Создаем ключ месяца (e.g., "2016-05")
        chunk['month_key'] = chunk[date_col].dt.to_period('M')

        # Группируем и сохраняем
        for month, group in chunk.groupby('month_key'):
            filename = f"bikes_{month}.csv"
            filepath = os.path.join(self.output_dir, filename)

            # Если файл не существует - пишем хедер, если существует - дописываем (append)
            header = not os.path.exists(filepath)

            # Удаляем временную колонку month_key перед сохранением
            group_to_save = group.drop(columns=['month_key'])
            group_to_save.to_csv(filepath, mode='a', header=header, index=False)