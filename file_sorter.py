#!/usr/bin/env python3
"""
Асинхронний сортувальник файлів за розширенням.
Читає файли з вихідної папки та розподіляє їх по підпапках у цільовій папці.
"""

import asyncio
import argparse
import logging
import shutil
from pathlib import Path
from typing import List, Set
import aiofiles
import aiofiles.os


# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_sorter.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def read_folder(source_path: Path) -> List[Path]:
    """
    Асинхронно рекурсивно читає всі файли у вихідній папці.
    
    Args:
        source_path: Шлях до вихідної папки
        
    Returns:
        List[Path]: Список шляхів до всіх файлів
    """
    files = []
    
    try:
        # Перевіряємо, чи існує папка
        if not await aiofiles.os.path.exists(source_path):
            logger.error(f"Вихідна папка не існує: {source_path}")
            return files
            
        if not await aiofiles.os.path.isdir(source_path):
            logger.error(f"Шлях не є папкою: {source_path}")
            return files
            
        # Рекурсивно обходимо всі файли та підпапки
        for item in source_path.rglob('*'):
            if await aiofiles.os.path.isfile(item):
                files.append(item)
                logger.debug(f"Знайдено файл: {item}")
                
        logger.info(f"Знайдено {len(files)} файлів у папці {source_path}")
        
    except Exception as e:
        logger.error(f"Помилка при читанні папки {source_path}: {e}")
        
    return files


async def copy_file(source_file: Path, output_folder: Path) -> bool:
    """
    Асинхронно копіює файл у відповідну підпапку на основі розширення.
    
    Args:
        source_file: Шлях до вихідного файла
        output_folder: Шлях до цільової папки
        
    Returns:
        bool: True якщо копіювання успішне, False інакше
    """
    try:
        # Отримуємо розширення файла (без крапки)
        file_extension = source_file.suffix.lower().lstrip('.')
        
        # Якщо розширення відсутнє, використовуємо папку "no_extension"
        if not file_extension:
            file_extension = 'no_extension'
            
        # Створюємо підпапку для розширення
        target_subfolder = output_folder / file_extension
        
        # Асинхронно створюємо папку, якщо вона не існує
        await aiofiles.os.makedirs(target_subfolder, exist_ok=True)
        
        # Визначаємо цільовий файл
        target_file = target_subfolder / source_file.name
        
        # Якщо файл з такою назвою вже існує, додаємо номер
        counter = 1
        original_target = target_file
        while await aiofiles.os.path.exists(target_file):
            stem = original_target.stem
            suffix = original_target.suffix
            target_file = target_subfolder / f"{stem}_{counter}{suffix}"
            counter += 1
            
        # Асинхронно копіюємо файл
        async with aiofiles.open(source_file, 'rb') as src:
            async with aiofiles.open(target_file, 'wb') as dst:
                while chunk := await src.read(8192):  # Читаємо по 8KB
                    await dst.write(chunk)
                    
        logger.info(f"Файл скопійовано: {source_file} -> {target_file}")
        return True
        
    except Exception as e:
        logger.error(f"Помилка при копіюванні файла {source_file}: {e}")
        return False


async def process_files(source_folder: Path, output_folder: Path, max_concurrent: int = 10):
    """
    Асинхронно обробляє всі файли з вихідної папки.
    
    Args:
        source_folder: Шлях до вихідної папки
        output_folder: Шлях до цільової папки
        max_concurrent: Максимальна кількість одночасних операцій
    """
    try:
        # Створюємо цільову папку, якщо вона не існує
        await aiofiles.os.makedirs(output_folder, exist_ok=True)
        
        # Читаємо всі файли з вихідної папки
        files = await read_folder(source_folder)
        
        if not files:
            logger.warning("Не знайдено файлів для обробки")
            return
            
        # Створюємо семафор для обмеження кількості одночасних операцій
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def copy_with_semaphore(file_path):
            async with semaphore:
                return await copy_file(file_path, output_folder)
        
        # Запускаємо копіювання всіх файлів одночасно
        tasks = [copy_with_semaphore(file_path) for file_path in files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Підраховуємо результати
        successful = sum(1 for result in results if result is True)
        failed = len(results) - successful
        
        logger.info(f"Обробка завершена. Успішно: {successful}, Помилок: {failed}")
        
        # Показуємо статистику по розширеннях
        extensions: Set[str] = set()
        for file_path in files:
            ext = file_path.suffix.lower().lstrip('.') or 'no_extension'
            extensions.add(ext)
            
        logger.info(f"Створено папок для розширень: {len(extensions)}")
        logger.info(f"Розширення: {', '.join(sorted(extensions))}")
        
    except Exception as e:
        logger.error(f"Критична помилка при обробці файлів: {e}")


def setup_argument_parser() -> argparse.ArgumentParser:
    """
    Налаштовує парсер аргументів командного рядка.
    
    Returns:
        ArgumentParser: Налаштований парсер аргументів
    """
    parser = argparse.ArgumentParser(
        description='Асинхронний сортувальник файлів за розширенням',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Приклади використання:
  python file_sorter.py /source/folder /output/folder
  python file_sorter.py ~/Downloads ~/Sorted --max-concurrent 20
  python file_sorter.py . ./sorted_files --verbose
        """
    )
    
    parser.add_argument(
        'source_folder',
        type=Path,
        help='Шлях до вихідної папки з файлами для сортування'
    )
    
    parser.add_argument(
        'output_folder', 
        type=Path,
        help='Шлях до цільової папки для відсортованих файлів'
    )
    
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=10,
        help='Максимальна кількість одночасних операцій копіювання (за замовчуванням: 10)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Увімкнути детальне логування'
    )
    
    return parser


async def main():
    """Головна асинхронна функція."""
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    # Налаштовуємо рівень логування
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        
    logger.info("Початок роботи асинхронного сортувальника файлів")
    logger.info(f"Вихідна папка: {args.source_folder.absolute()}")
    logger.info(f"Цільова папка: {args.output_folder.absolute()}")
    logger.info(f"Максимальна кількість одночасних операцій: {args.max_concurrent}")
    
    # Валідація вихідної папки
    if not args.source_folder.exists():
        logger.error(f"Вихідна папка не існує: {args.source_folder}")
        return
        
    if not args.source_folder.is_dir():
        logger.error(f"Вказаний шлях не є папкою: {args.source_folder}")
        return
    
    # Запускаємо обробку файлів
    await process_files(
        args.source_folder, 
        args.output_folder, 
        args.max_concurrent
    )
    
    logger.info("Робота завершена")


if __name__ == '__main__':
    # Запускаємо асинхронну головну функцію
    asyncio.run(main())