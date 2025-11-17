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
import os
import shutil

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def search_duckduckgo(query: str, num_results: int = 10) -> list[str]:
    """Поиск через DuckDuckGo API (библиотека duckduckgo-search)"""
    try:
        logger.debug(f"DuckDuckGo API запрос: {query}")
        
        with DDGS() as ddgs:
            # Увеличиваем количество результатов и добавляем регион для русскоязычных сайтов
            results = list(ddgs.text(query, region='ru-ru', max_results=num_results * 3))
            links = [r['href'] for r in results if 'href' in r]
            
            # Фильтруем Wikipedia и Wiktionary
            filtered_links = [
                link for link in links 
                if not any(wiki in link.lower() for wiki in ['wikipedia.org', 'wiktionary.org', 'wiki'])
            ]
            
            logger.debug(f"DuckDuckGo API: найдено {len(links)} ссылок, после фильтрации: {len(filtered_links)}")
            return filtered_links[:num_results]
                    
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
                return text[:5000]  # Увеличиваем лимит до 5000 символов для большей информативности
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


def search_internet(query: str, num_results: int = 10, search_engines: List[str] = None) -> tuple[list[str], list[str]]:
    """
    Поиск информации в интернете по запросу с использованием DuckDuckGo
    
    Args:
        query: Поисковый запрос
        num_results: Количество результатов для обработки (по умолчанию: 10)
        search_engines: Список поисковиков (игнорируется, используется только DuckDuckGo)
        
    Returns:
        Кортеж (список текстов с найденных страниц, список ссылок на источники)
    """
    results = []
    used_links = []
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
            used_links.append(link)
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
    
    return results, used_links


def generate_description_with_confidence(
    product_name: str,
    model: str = "llama3.2",
    host: Optional[str] = None,
    comment: str = None
) -> tuple[str, float]:
    """
    Генерация описания товара с оценкой уверенности модели
    
    Args:
        product_name: Название товара
        model: Модель Ollama для использования
        host: Хост Ollama
        comment: Дополнительный комментарий о товаре
        
    Returns:
        Кортеж (описание, уверенность в диапазоне 0-1)
    """
    client = ollama.Client(host=host) if host else ollama.Client()
    
    comment_context = ""
    if comment and str(comment).strip():
        comment_context = f"\n\nДополнительная информация: {comment}"
    
    # Промпт с запросом на оценку уверенности
    prompt = f"""Объект: {product_name}{comment_context}

Задание: 
1. Создай краткое описание (2-4 предложения): что это, где применяется, для чего используется
2. Оцени свою уверенность в описании от 0.0 до 1.0, где:
   - 0.9-1.0: высокая уверенность, знаю этот товар/услугу хорошо
   - 0.7-0.9: средняя уверенность, общее представление есть
   - 0.0-0.7: низкая уверенность, нужна дополнительная информация

Формат ответа:
ОПИСАНИЕ: [твое описание]
УВЕРЕННОСТЬ: [число от 0.0 до 1.0]"""
    
    system_prompt = """Ты - эксперт по техническим товарам, строительным материалам, промышленному оборудованию и услугам.
Всегда давай конкретное описание на основе своих знаний и честно оценивай свою уверенность."""
    
    try:
        response = client.generate(
            model=model,
            prompt=prompt,
            system=system_prompt,
            options={
                'temperature': 0.3,
                'num_predict': 300
            }
        )
        
        result = response['response'].strip()
        
        # Парсим ответ
        description = ""
        confidence = 0.0
        
        for line in result.split('\n'):
            if line.startswith('ОПИСАНИЕ:'):
                description = line.replace('ОПИСАНИЕ:', '').strip()
            elif line.startswith('УВЕРЕННОСТЬ:'):
                try:
                    conf_str = line.replace('УВЕРЕННОСТЬ:', '').strip()
                    confidence = float(conf_str)
                except:
                    confidence = 0.5  # По умолчанию средняя уверенность
        
        # Если формат не распознан, пытаемся извлечь описание
        if not description:
            description = result.split('\n')[0]  # Берем первую строку
            confidence = 0.5
        
        return description, confidence
        
    except Exception as e:
        logger.error(f"Ошибка при генерации с оценкой уверенности: {e}")
        return "", 0.0


def summarize_with_ollama(
    product_name: str, 
    search_results: list[str], 
    model: str = "llama3.2",
    host: Optional[str] = None,
    comment: str = None
) -> str:
    """
    Суммаризация информации о товаре с помощью Ollama
    
    Args:
        product_name: Название товара
        search_results: Результаты поиска
        model: Модель Ollama для использования
        host: Хост Ollama (например, 'http://localhost:11434')
        comment: Дополнительный комментарий о товаре (опционально)
        
    Returns:
        Краткое описание товара
    """
    # Создаем клиент с указанным хостом, если он задан
    client = ollama.Client(host=host) if host else ollama.Client()
    
    # Системный промпт с экспертностью
    system_prompt = """Ты - эксперт по техническим товарам, строительным материалам, промышленному оборудованию и услугам. 
У тебя глубокие знания в области:
- Электротехники и электроники
- Строительных материалов и инструментов
- Промышленного оборудования и запчастей
- Офисных и канцелярских товаров
- Расходных материалов
- Производственных и технических услуг
- Строительных и монтажных работ

Твоя задача - создавать краткие, информативные и понятные описания товаров и услуг. 
Всегда давай конкретный ответ, используй профессиональную терминологию, но объясняй простым языком.
Никогда не говори что не можешь найти информацию - всегда используй свои знания для создания описания."""
    
    # Добавляем комментарий к контексту, если он есть
    comment_context = ""
    if comment and str(comment).strip():
        comment_context = f"\n\nДополнительная информация о товаре:\n{comment}"
    
    # Если есть результаты поиска - используем их
    if search_results:
        # Объединяем результаты поиска (берем больше результатов для лучшего качества)
        context = "\n\n".join(search_results[:5])  # Увеличиваем до 5 результатов
        
        prompt = f"""Объект для описания: {product_name}

Справочная информация из интернета (используй ее как источник знаний):
{context}{comment_context}

Задание: Создай описание для "{product_name}" (2-4 предложения). 
Это может быть товар или услуга.
- Опиши ЧТО ЭТО (товар или услуга)
- ГДЕ применяется или используется
- ДЛЯ ЧЕГО нужно
- Используй факты из справочной информации, но описывай именно "{product_name}", а не сайты или статьи

Описание "{product_name}":"""
    else:
        # Если информации нет - попробуем создать описание на основе названия
        prompt = f"""Объект для описания: {product_name}{comment_context}

Задание: Создай описание для "{product_name}" (2-4 предложения).
Это может быть товар или услуга.
- Проанализируй название
- Опиши ЧТО ЭТО (товар или услуга)
- ГДЕ применяется или используется
- ДЛЯ ЧЕГО нужно

Описание "{product_name}":"""
    
    try:
        response = client.generate(
            model=model,
            prompt=prompt,
            system=system_prompt,
            options={
                'temperature': 0.5,
                'num_predict': 250,
                'top_p': 0.9
            }
        )
        
        description = response['response'].strip()
        
        # Убираем фразы типа "я не знаю", "не удалось найти" и т.п.
        negative_phrases = [
            "я не смог", "я не могу", "не удалось найти", "нет информации",
            "недостаточно информации", "к сожалению", "не могу предоставить",
            "предоставьте название", "укажите название", "пожалуйста, предоставьте"
        ]
        
        # Если описание содержит негативные фразы или слишком короткое, используем fallback
        if any(phrase.lower() in description.lower() for phrase in negative_phrases) or len(description) < 20:
            logger.warning(f"  - ⚠️ Модель дала неудовлетворительный ответ, используем упрощенный подход...")
            
            # Упрощенный промпт без лишних инструкций
            simple_prompt = f"""Объект: {product_name}

Опиши кратко (2-3 предложения): что это (товар или услуга), где применяется, для чего используется.{comment_context if comment else ""}

Описание:"""
            
            response = client.generate(
                model=model,
                prompt=simple_prompt,
                system=system_prompt,
                options={
                    'temperature': 0.7,
                    'num_predict': 200
                }
            )
            description = response['response'].strip()
        
        return description
    
    except Exception as e:
        logger.error(f"Ошибка при обращении к Ollama: {e}")
        return f"Ошибка суммаризации: {str(e)}"


def rotate_backup_files(output_file: str):
    """
    Ротация файлов: текущий → предыдущий, старый предыдущий → удаляется
    
    Args:
        output_file: Путь к выходному файлу
    """
    previous_file = output_file.replace('.xlsx', '_previous.xlsx')
    
    # Если существует текущий файл, делаем его предыдущим
    if os.path.exists(output_file):
        # Удаляем старый предыдущий файл, если он есть
        if os.path.exists(previous_file):
            try:
                os.remove(previous_file)
                logger.debug(f"Удален старый бэкап: {previous_file}")
            except Exception as e:
                logger.warning(f"Не удалось удалить старый бэкап: {e}")
        
        # Переименовываем текущий в предыдущий
        try:
            shutil.copy2(output_file, previous_file)
            logger.debug(f"Создан бэкап: {previous_file}")
        except Exception as e:
            logger.warning(f"Не удалось создать бэкап: {e}")


def process_excel(
    input_file: str,
    output_file: str,
    column_name: str = "Полное наименование",
    description_column: str = "Расшифровка",
    sources_column: str = "Источники",
    confidence_column: str = "Уверенность модели",
    ollama_host: Optional[str] = None,
    ollama_model: str = "llama3.2",
    skip_existing: bool = True,
    confidence_threshold: float = 0.7
):
    """
    Обработка Excel файла с добавлением описаний
    
    Args:
        input_file: Путь к входному файлу
        output_file: Путь к выходному файлу
        column_name: Название колонки с наименованиями товаров
        description_column: Название новой колонки с описаниями
        sources_column: Название колонки со ссылками на источники
        confidence_column: Название колонки с уверенностью модели
        ollama_host: Хост Ollama
        ollama_model: Модель Ollama
        skip_existing: Пропускать уже обработанные строки
        confidence_threshold: Порог уверенности модели (0.0-1.0) для обращения к интернету
    """
    logger.info(f"Загрузка файла: {input_file}")
    
    # Проверяем наличие предыдущей версии файла для возобновления работы
    previous_file = output_file.replace('.xlsx', '_previous.xlsx')
    start_from_index = 0
    
    if os.path.exists(previous_file):
        logger.info(f"🔄 Обнаружен файл предыдущего запуска: {previous_file}")
        try:
            # Загружаем предыдущую версию
            df_previous = pd.read_excel(previous_file)
            
            # Определяем последнюю обработанную строку
            if description_column in df_previous.columns:
                # Ищем последнюю строку с заполненным описанием
                for idx in range(len(df_previous) - 1, -1, -1):
                    if pd.notna(df_previous.at[idx, description_column]) and df_previous.at[idx, description_column].strip():
                        start_from_index = idx + 1
                        break
                
                if start_from_index > 0:
                    logger.info(f"✅ Найдена последняя обработанная строка: {start_from_index}")
                    logger.info(f"🚀 Возобновление работы со строки {start_from_index + 1}")
                else:
                    logger.info("ℹ️ В предыдущем файле нет обработанных строк, начинаем с начала")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось загрузить предыдущий файл: {e}")
            logger.info("Начинаем обработку с начала")
    else:
        logger.info("ℹ️ Файл предыдущего запуска не найден, начинаем с начала")
    
    # Читаем Excel файл
    df = pd.read_excel(input_file)
    
    # Проверяем наличие колонки
    if column_name not in df.columns:
        raise ValueError(f"Колонка '{column_name}' не найдена в файле")
    
    # Добавляем колонку для описаний, если её нет
    if description_column not in df.columns:
        df[description_column] = ""
    
    # Добавляем колонку для источников, если её нет
    if sources_column not in df.columns:
        df[sources_column] = ""
    
    # Добавляем колонку для уверенности, если её нет
    if confidence_column not in df.columns:
        df[confidence_column] = ""
    
    # Если есть предыдущая версия, копируем уже обработанные данные
    if start_from_index > 0 and os.path.exists(previous_file):
        try:
            df_previous = pd.read_excel(previous_file)
            if description_column in df_previous.columns:
                # Копируем обработанные описания из предыдущей версии
                for idx in range(min(start_from_index, len(df_previous), len(df))):
                    if pd.notna(df_previous.at[idx, description_column]):
                        df.at[idx, description_column] = df_previous.at[idx, description_column]
                    # Копируем источники, если они есть
                    if sources_column in df_previous.columns and pd.notna(df_previous.at[idx, sources_column]):
                        df.at[idx, sources_column] = df_previous.at[idx, sources_column]
                    # Копируем уверенность, если она есть
                    if confidence_column in df_previous.columns and pd.notna(df_previous.at[idx, confidence_column]):
                        df.at[idx, confidence_column] = df_previous.at[idx, confidence_column]
                logger.info(f"📋 Скопировано {start_from_index} обработанных описаний из предыдущего файла")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось скопировать данные из предыдущего файла: {e}")
    
    # Определяем колонку для фильтрации
    production_filter_column = "Вид воспроизводства"
    nomenclature_type_column = "Вид номенклатуры"
    
    logger.info(f"Найдено {len(df)} строк для обработки")
    
    # Подсчитываем количество строк для обработки
    total_to_process = 0
    for idx in range(start_from_index, len(df)):
        row = df.iloc[idx]
        product_name = row[column_name]
        
        # Пропускаем специальные случаи (производство, служебные фразы, пустые)
        if production_filter_column in df.columns and row[production_filter_column] == "Производство":
            continue
        # Пропускаем если не товар
        if nomenclature_type_column in df.columns and row[nomenclature_type_column] != "Товар":
            continue
        if pd.notna(product_name):
            product_name_lower = str(product_name).lower()
            skip_phrases = ["использовать", "не заполнять, висят док-ты по отгрузке"]
            if any(phrase in product_name_lower for phrase in skip_phrases):
                continue
        if pd.isna(product_name) or not str(product_name).strip():
            continue
        if skip_existing and pd.notna(row[description_column]) and row[description_column].strip():
            continue
        
        total_to_process += 1
    
    if start_from_index > 0:
        logger.info(f"📊 Пропущено уже обработанных строк: {start_from_index}")
        logger.info(f"📊 Осталось обработать: {total_to_process} строк")
    else:
        logger.info(f"📊 Всего строк для обработки: {total_to_process}")
    
    # Обрабатываем каждую строку, начиная с нужного индекса
    processed_count = 0
    for idx, row in df.iterrows():
        # Пропускаем строки до start_from_index
        if idx < start_from_index:
            continue
        
        product_name = row[column_name]
        
        # Проверяем на специальные фразы в полном наименовании
        if pd.notna(product_name):
            product_name_lower = str(product_name).lower()
            skip_phrases = ["использовать", "не заполнять, висят док-ты по отгрузке"]
            
            if any(phrase in product_name_lower for phrase in skip_phrases):
                logger.info(f"[{idx+1}/{len(df)}] Найдена служебная фраза, пропуск")
                continue
        
        # Пропускаем строки с "Вид воспроизводства" = "Производство"
        if production_filter_column in df.columns and row[production_filter_column] == "Производство":
            logger.info(f"[{idx+1}/{len(df)}] Пропуск (Вид воспроизводства = Производство): {product_name}")
            continue
        
        # Обрабатываем только товары
        if nomenclature_type_column in df.columns:
            if pd.isna(row[nomenclature_type_column]) or row[nomenclature_type_column] != "Товар":
                nomenclature_type = row[nomenclature_type_column] if pd.notna(row[nomenclature_type_column]) else "пусто"
                logger.info(f"[{idx+1}/{len(df)}] Пропуск (Вид номенклатуры = {nomenclature_type}): {product_name}")
                continue
        
        # Пропускаем, если уже есть описание и включен режим пропуска
        if skip_existing and pd.notna(row[description_column]) and row[description_column].strip():
            logger.info(f"[{idx+1}/{len(df)}] Пропуск (уже обработано): {product_name}")
            continue
        
        if pd.isna(product_name) or not str(product_name).strip():
            logger.info(f"[{idx+1}/{len(df)}] Пропуск (пустое название)")
            continue
        
        logger.info(f"[{idx+1}/{len(df)}] Обработка: {product_name}")
        processed_count += 1
        logger.info(f"  - 📈 Прогресс: {processed_count}/{total_to_process} строк")
        
        # Получаем комментарий, если он есть
        comment = None
        comment_column = "Комментарий"
        if comment_column in df.columns and pd.notna(row[comment_column]):
            comment = str(row[comment_column]).strip()
            if comment:
                logger.info(f"  - 📝 Найден комментарий: {comment[:100]}...")
        
        try:
            # Шаг 1: Пытаемся создать описание без интернета
            logger.info("  - 🤖 Попытка создать описание на основе знаний модели...")
            initial_description, confidence = generate_description_with_confidence(
                str(product_name),
                model=ollama_model,
                host=ollama_host,
                comment=comment
            )
            
            logger.info(f"  - 📊 Уверенность модели: {confidence:.2f}")
            
            # Порог уверенности для обращения к интернету
            
            if confidence >= confidence_threshold:
                # Модель уверена - используем её ответ
                logger.info(f"  - ✅ Модель уверена (≥{confidence_threshold}), используем её описание")
                description = initial_description
                source_links = []
                search_results = []
            else:
                # Модель не уверена - ищем в интернете
                logger.info(f"  - 🔍 Модель не уверена (<{confidence_threshold}), обращаемся к интернету...")
                search_results, source_links = search_internet(str(product_name))
                
                if search_results:
                    logger.info(f"  - ✅ Найдено {len(search_results)} релевантных страниц с информацией")
                    # Суммаризация с использованием интернет-данных
                    logger.info("  - Создание описания на основе интернет-данных...")
                    description = summarize_with_ollama(
                        str(product_name), 
                        search_results,
                        model=ollama_model,
                        host=ollama_host,
                        comment=comment
                    )
                else:
                    logger.warning("  - ⚠️ Информация в интернете НЕ НАЙДЕНА")
                    logger.warning("      Используем первоначальное описание модели")
                    description = initial_description
            
            df.at[idx, description_column] = description
            
            # Сохраняем уверенность модели
            df.at[idx, confidence_column] = f"{confidence:.2f}"
            
            # Сохраняем ссылки на источники
            if source_links:
                df.at[idx, sources_column] = ", ".join(source_links)
            elif confidence >= confidence_threshold:
                df.at[idx, sources_column] = f"Создано LLM (уверенность: {confidence:.2f})"
            else:
                df.at[idx, sources_column] = f"Создано LLM без интернета (уверенность: {confidence:.2f})"
            
            # Показываем результат с пометкой об источнике информации
            if search_results:
                source_mark = "🌐"
            elif confidence >= confidence_threshold:
                source_mark = "🤖✓"
            else:
                source_mark = "🤖"
            logger.info(f"  - {source_mark} Готово: {description[:100]}...")
            
            # Сохраняем промежуточный результат с ротацией бэкапов
            try:
                # Создаем бэкап перед сохранением
                rotate_backup_files(output_file)
                
                # Сохраняем текущее состояние
                df.to_excel(output_file, index=False)
                logger.debug(f"  - 💾 Прогресс сохранен")
            except Exception as e:
                logger.error(f"  - ⚠️ Ошибка сохранения: {e}")
                logger.error(f"     Данные НЕ потеряны, но файл может быть поврежден")
            
            # Небольшая пауза между запросами
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"  - Ошибка при обработке: {e}")
            df.at[idx, description_column] = f"Ошибка: {str(e)}"
            
            # Даже при ошибке обработки пытаемся сохранить
            try:
                rotate_backup_files(output_file)
                df.to_excel(output_file, index=False)
            except Exception as save_error:
                logger.error(f"  - ⚠️ Критическая ошибка сохранения: {save_error}")
            
            continue
    
    # Финальное сохранение
    try:
        rotate_backup_files(output_file)
        df.to_excel(output_file, index=False)
        logger.info(f"✅ Результат сохранен в: {output_file}")
        
        previous_file = output_file.replace('.xlsx', '_previous.xlsx')
        if os.path.exists(previous_file):
            logger.info(f"💾 Бэкап доступен в: {previous_file}")
    except Exception as e:
        logger.error(f"❌ Ошибка финального сохранения: {e}")
        previous_file = output_file.replace('.xlsx', '_previous.xlsx')
        if os.path.exists(previous_file):
            logger.warning(f"⚠️ Используйте бэкап: {previous_file}")


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
        '--sources-column',
        type=str,
        default='Источники',
        help='Название колонки со ссылками на источники (по умолчанию: Источники)'
    )
    parser.add_argument(
        '--confidence-column',
        type=str,
        default='Уверенность модели',
        help='Название колонки с уверенностью модели (по умолчанию: Уверенность модели)'
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
    parser.add_argument(
        '--confidence-threshold',
        type=float,
        default=0.7,
        help='Порог уверенности модели (0.0-1.0) для обращения к интернету (по умолчанию: 0.7)'
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
            sources_column=args.sources_column,
            confidence_column=args.confidence_column,
            ollama_host=args.ollama_host,
            ollama_model=args.ollama_model,
            skip_existing=not args.no_skip_existing,
            confidence_threshold=args.confidence_threshold
        )
        logger.info("Обработка завершена успешно!")
    except Exception as e:
        logger.error(f"Ошибка при обработке: {e}")
        raise


if __name__ == "__main__":
    main()
