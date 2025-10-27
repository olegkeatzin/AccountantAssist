import argparse
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import quote_plus
import ollama
from typing import Optional, List
import logging
import json
from ddgs import DDGS

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def search_duckduckgo(query: str, num_results: int = 3) -> list[str]:
    """Поиск через DuckDuckGo API (библиотека duckduckgo-search)"""
    try:
        logger.debug(f"DuckDuckGo API запрос: {query}")
        
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=num_results * 2))
            links = [r['href'] for r in results if 'href' in r]
            
            logger.debug(f"DuckDuckGo API: найдено {len(links)} ссылок")
            return links[:num_results]
                    
    except Exception as e:
        logger.warning(f"DuckDuckGo API ошибка: {e}")
    
    return []


def fetch_page_content(url: str, headers: dict) -> str:
    """Получить содержимое страницы"""
    try:
        logger.debug(f"Получение страницы: {url[:60]}...")
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Удаляем скрипты и стили
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            # Извлекаем текст из параграфов и других элементов
            text_elements = soup.find_all(['p', 'div', 'span', 'article', 'section', 'h1', 'h2', 'h3', 'li'])
            text = ' '.join([elem.get_text().strip() for elem in text_elements if elem.get_text().strip()])
            
            # Очищаем текст
            text = ' '.join(text.split())  # Убираем лишние пробелы
            
            if len(text) > 100:
                logger.debug(f"✓ Извлечено {len(text)} символов")
                return text[:3000]  # Увеличил лимит до 3000 символов
            else:
                logger.debug(f"✗ Мало текста ({len(text)} символов)")
                logger.debug(f"   Первые 200 символов: {text[:200]}")
        else:
            logger.debug(f"✗ Код ответа: {response.status_code}")
            
    except requests.Timeout:
        logger.debug(f"⏱️ Таймаут")
    except Exception as e:
        logger.debug(f"✗ Ошибка: {e}")
    
    return ""


def search_internet(query: str, num_results: int = 3, search_engines: List[str] = None) -> list[str]:
    """
    Поиск информации в интернете по запросу с использованием DuckDuckGo
    
    Args:
        query: Поисковый запрос
        num_results: Количество результатов для обработки
        search_engines: Список поисковиков (игнорируется, используется только DuckDuckGo)
        
    Returns:
        Список текстов с найденных страниц (может быть пустым)
    """
    results = []
    all_links = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    logger.debug(f"Поиск через DuckDuckGo API...")
    
    try:
        all_links = search_duckduckgo(query, num_results)
        
        if all_links:
            logger.info(f"  - DuckDuckGo: найдено {len(all_links)} ссылок")
        else:
            logger.debug(f"  - DuckDuckGo: ссылок не найдено")
                
    except Exception as e:
        logger.warning(f"  - DuckDuckGo: ошибка {e}")
    
    if not all_links:
        logger.warning("⚠️ Поисковик не вернул ссылок")
        return results
    
    # Убираем дубликаты
    all_links = list(dict.fromkeys(all_links))
    logger.info(f"  - Найдено уникальных ссылок: {len(all_links)}")
    
    # Выводим ссылки в DEBUG режиме
    for i, link in enumerate(all_links[:num_results], 1):
        logger.debug(f"  Ссылка {i}: {link}")
    
    # Получаем содержимое страниц
    for i, link in enumerate(all_links[:num_results], 1):
        logger.info(f"  - Извлечение текста со страницы {i}/{min(len(all_links), num_results)}: {link[:80]}...")
        content = fetch_page_content(link, headers)
        if content:
            results.append(content)
            logger.info(f"    ✓ Успешно извлечено {len(content)} символов")
        else:
            logger.warning(f"    ✗ Не удалось извлечь текст")
        
        time.sleep(1)  # Пауза между запросами
        
        # Если уже получили достаточно результатов
        if len(results) >= num_results:
            break
    
    if not results:
        logger.warning("⚠️ Не удалось извлечь текст ни с одной страницы")
    else:
        logger.info(f"  - ✅ Успешно извлечено {len(results)} страниц с содержимым")
    
    return results


def summarize_with_ollama(
    product_name: str, 
    search_results: list[str], 
    model: str = "llama3.2",
    host: Optional[str] = None
) -> str:
    """
    Суммаризация информации о товаре с помощью Ollama
    
    Args:
        product_name: Название товара
        search_results: Результаты поиска
        model: Модель Ollama для использования
        host: Хост Ollama (например, 'http://localhost:11434')
        
    Returns:
        Краткое описание товара
    """
    # Создаем клиент с указанным хостом, если он задан
    client = ollama.Client(host=host) if host else ollama.Client()
    
    # Если есть результаты поиска - используем их
    if search_results:
        # Объединяем результаты поиска
        context = "\n\n".join(search_results[:3])  # Берем первые 3 результата
        
        prompt = f"""На основе следующей информации из интернета, создай краткое описание товара "{product_name}".

Информация из интернета:
{context}

Краткое описание товара:"""
    else:
        # Если информации нет - попробуем создать описание на основе названия
        prompt = f"""На основе названия товара "{product_name}", создай краткое описание.
Если ты не знаешь что это за товар, попробуй разобрать название по частям и объяснить.
Краткое описание товара:"""
    
    try:
        response = client.generate(
            model=model,
            prompt=prompt,
            options={
                'temperature': 0.3,
                'num_predict': 200
            }
        )
        
        description = response['response'].strip()
        
        # Если информации из интернета не было - добавляем пометку
        if not search_results:
            description = f"[На основе названия] {description}"
        
        return description
    
    except Exception as e:
        logger.error(f"Ошибка при обращении к Ollama: {e}")
        return f"Ошибка суммаризации: {str(e)}"


def process_excel(
    input_file: str,
    output_file: str,
    column_name: str = "Полное наименование",
    description_column: str = "Расшифровка",
    ollama_host: Optional[str] = None,
    ollama_model: str = "llama3.2",
    skip_existing: bool = True
):
    """
    Обработка Excel файла с добавлением описаний
    
    Args:
        input_file: Путь к входному файлу
        output_file: Путь к выходному файлу
        column_name: Название колонки с наименованиями товаров
        description_column: Название новой колонки с описаниями
        ollama_host: Хост Ollama
        ollama_model: Модель Ollama
        skip_existing: Пропускать уже обработанные строки
    """
    logger.info(f"Загрузка файла: {input_file}")
    
    # Читаем Excel файл
    df = pd.read_excel(input_file)
    
    # Проверяем наличие колонки
    if column_name not in df.columns:
        raise ValueError(f"Колонка '{column_name}' не найдена в файле")
    
    # Добавляем колонку для описаний, если её нет
    if description_column not in df.columns:
        df[description_column] = ""
    
    logger.info(f"Найдено {len(df)} строк для обработки")
    
    # Обрабатываем каждую строку
    for idx, row in df.iterrows():
        product_name = row[column_name]
        
        # Пропускаем, если уже есть описание и включен режим пропуска
        if skip_existing and pd.notna(row[description_column]) and row[description_column].strip():
            logger.info(f"[{idx+1}/{len(df)}] Пропуск (уже обработано): {product_name}")
            continue
        
        if pd.isna(product_name) or not str(product_name).strip():
            logger.info(f"[{idx+1}/{len(df)}] Пропуск (пустое название)")
            continue
        
        logger.info(f"[{idx+1}/{len(df)}] Обработка: {product_name}")
        
        try:
            # Поиск информации
            logger.info("  - Поиск информации в интернете...")
            search_results = search_internet(str(product_name))
            
            if search_results:
                logger.info(f"  - ✅ Найдено {len(search_results)} релевантных страниц с информацией")
            else:
                logger.warning("  - ⚠️ Информация в интернете НЕ НАЙДЕНА")
                logger.warning("      Причины могут быть:")
                logger.warning("      • Товар слишком специфический/редкий")
                logger.warning("      • Поисковик не нашел подходящих страниц")
                logger.warning("      • Найденные страницы недоступны или пусты")
                logger.warning("      • Проблемы с интернет-соединением")
                logger.warning("      📝 Будет создано описание на основе НАЗВАНИЯ товара (LLM)")
            
            # Суммаризация с помощью Ollama (работает даже без результатов поиска)
            logger.info("  - Суммаризация с помощью Ollama...")
            description = summarize_with_ollama(
                str(product_name), 
                search_results,
                model=ollama_model,
                host=ollama_host
            )
            
            df.at[idx, description_column] = description
            
            # Показываем результат с пометкой об источнике информации
            source_mark = "🌐" if search_results else "🤖"
            logger.info(f"  - {source_mark} Готово: {description[:100]}...")
            
            # Сохраняем промежуточный результат после каждой обработанной строки
            df.to_excel(output_file, index=False)
            
            # Небольшая пауза между запросами
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"  - Ошибка при обработке: {e}")
            df.at[idx, description_column] = f"Ошибка: {str(e)}"
            continue
    
    # Финальное сохранение
    df.to_excel(output_file, index=False)
    logger.info(f"Результат сохранен в: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Добавление описаний товаров в Excel файл с использованием интернет-поиска и Ollama'
    )
    parser.add_argument(
        '--input',
        type=str,
        default='Номенклатура полная.xlsx',
        help='Путь к входному Excel файлу (по умолчанию: Номенклатура полная.xlsx)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='Номенклатура полная с описаниями.xlsx',
        help='Путь к выходному Excel файлу (по умолчанию: Номенклатура полная с описаниями.xlsx)'
    )
    parser.add_argument(
        '--column',
        type=str,
        default='Полное наименование',
        help='Название колонки с наименованиями товаров (по умолчанию: Полное наименование)'
    )
    parser.add_argument(
        '--description-column',
        type=str,
        default='Расшифровка',
        help='Название новой колонки с описаниями (по умолчанию: Расшифровка)'
    )
    parser.add_argument(
        '--ollama-host',
        type=str,
        default=None,
        help='Хост Ollama (например: http://localhost:11434)'
    )
    parser.add_argument(
        '--ollama-model',
        type=str,
        default='llama3.2',
        help='Модель Ollama (по умолчанию: llama3.2)'
    )
    parser.add_argument(
        '--no-skip-existing',
        action='store_true',
        help='Перезаписывать существующие описания'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Подробное логирование (показывать детали поиска)'
    )
    
    args = parser.parse_args()
    
    # Устанавливаем уровень логирования
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.info("🔍 Включен режим подробного логирования")
    
    logger.info("✅ Используется DuckDuckGo Search API (библиотека ddgs)")
    
    try:
        process_excel(
            input_file=args.input,
            output_file=args.output,
            column_name=args.column,
            description_column=args.description_column,
            ollama_host=args.ollama_host,
            ollama_model=args.ollama_model,
            skip_existing=not args.no_skip_existing
        )
        logger.info("Обработка завершена успешно!")
    except Exception as e:
        logger.error(f"Ошибка при обработке: {e}")
        raise


if __name__ == "__main__":
    main()
