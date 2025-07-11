# DrugDesign Data Analysis

[![License: Unlicense](https://img.shields.io/badge/license-Unlicense-green.svg)](http://unlicense.org/)
[![Contributing](https://img.shields.io/badge/contributing-.md-blue.svg)](./CONTRIBUTING.md)

templated: generated from [UmbrellaLeaf5/template_python_usual](https://github.com/UmbrellaLeaf5/template_python_usual)

___

## Contents
  * [Description](#description)
  * [Documentation](#documentation)
  * [Installation](#installation)
  * [Configurations](#configurations)
  * [Sources](#sources)

## Description

Этот модуль отвечает за загрузку и предварительную обработку данных из [ChEMBL](#sources) и [PubChem](#sources), необходимых для дальнейшего моделирования и анализа при разработке лекарств.

Цель модуля:
* Автоматическое скачивание данных о соединениях, активностях и мишенях
  из базы данных ChEMBL, а также данных о токсичности соединений из PubChem ChemIDplus.
* Преобразование данных в формат, пригодный для обучения моделей машинного
  обучения.
* Фильтрация и очистка данных для повышения качества моделей.

## Documentation

[Документация](https://umbrellaleaf5.github.io/DrugDesign_data_analysis), созданная с помощью [Doxygen](https://www.doxygen.nl).

## Installation

0.  **Клонирование репозитория:**

    Перед тем как начать, вам необходимо клонировать репозиторий с исходным кодом проекта.
 
    ```bash
    git clone https://gitlab.com/UmbrellaLeaf5/drugdesign_parsing.git
    ```

    Перейдите в директорию, куда был клонирован репозиторий:

    ```bash
    cd DrugDesign_data_analysis
    ``` 


1.  **Создание виртуального окружения:**

    Откройте терминал или командную строку в корневой директории вашего проекта (там, где находится файл `requirements.txt`) и выполните следующую команду для создания виртуального окружения с именем `.venv`:

    ```bash
    python3 -m venv .venv
    ```

    или

    ```bash
    python -m venv .venv
    ```

    *   Если у вас установлена только версия `Python 3`, можете использовать `python` вместо `python3`.
    *   Если виртуальное окружение уже существует (вы его создавали ранее), пропустите этот шаг.


2.  **Активация виртуального окружения:**

    Активируйте виртуальное окружение, чтобы `Python` использовал библиотеки, установленные внутри него:

    *   **Linux/macOS:**

        ```bash
        source .venv/bin/activate
        ```

    *   **Windows (Command Prompt):**

        ```cmd
        .venv\Scripts\activate
        ```

    *   **Windows (PowerShell):**

        ```powershell
        .venv\Scripts\Activate.ps1
        ```

    После активации вы увидите `(.venv)` в начале строки терминала, указывающее на то, что виртуальное окружение активно.


3.  **Установка зависимостей из `requirements.txt`:**

    Установите все библиотеки, перечисленные в файле `requirements.txt`, выполнив следующую команду:

    ```bash
    ./.venv/Scripts/pip install -r requirements.txt
    ```

    или просто:

    ```bash
    pip install -r requirements.txt
    ```
**Или просто используйте [`uv`](https://github.com/astral-sh/uv):**

```bash
uv venv .venv
```

*   **Linux/macOS:**

    ```bash
    source .venv/bin/activate
    ```

*   **Windows (Git Bash):**

    ```bash
    source .venv/Scripts/activate
    ```

*   **Windows (Command Prompt):**

    ```cmd
    .venv\Scripts\activate
    ```

*   **Windows (PowerShell):**

    ```powershell
    .venv\Scripts\Activate.ps1
    ```

```bash
uv pip install -r requirements.txt
```

P.S. одной строкой:
```bash
uv venv .venv && source .venv/Scripts/activate && uv pip install -r requirements.txt
```

## Configurations

Файл [`config.json`](./Configurations/config.json) содержит параметры конфигурации для загрузки и обработки данных, в основном сфокусированного на базах данных `ChEMBL` и `PubChem`. Он определяет настройки для загрузки соединений, активностей, клеточных линий и информации о мишенях из `ChEMBL`, а также данных о токсичности из `PubChem`.

### Общие настройки

*   `testing_flag`: *boolean* - логический флаг, который, когда установлен в `true`, включает режим тестирования (ограничивает количество загружаемых данных для ускорения тестирования). При рабочей эксплуатации должен быть установлен в `false`.
*   `skip_downloaded`: *boolean* - логический флаг, указывающий, следует ли пропускать загрузку, если файлы уже существуют (полезно для повторного запуска скрипта без перезагрузки данных).
*   `csv_separator`: *string* - строка, определяющая разделитель полей в CSV-файлах (например, "," или ";").
*   `keyboard_end_message`: *string* - сообщение, которое выводится при завершении загрузки по нажатию клавиш (`Ctrl+C`/`Ctrl+Z`).

### ChEMBL

#### ChEMBL_download_activities

Подзадача загрузки активностей с ChEMBL для различных мишеней и клеточных линий, которая выполняется при включении определенных флагов в других задачах.

*   `logger_label`: *string* - метка, используемая для сообщений журнала, связанных с этой задачей (для идентификации в логах).
*   `logger_color`: *string* - цветовой код для вывода журнала.
*   `results_folder_name`: *string* - имя папки для хранения загруженных данных об активности.
*   `download_compounds_sdf`: *boolean* - логический флаг, указывающий, следует ли догружать соединения в формате SDF.
*   `filtering`: *dictionary* - словарь, содержащий параметры фильтрации данных об активностях.
    *   `targets`: *dictionary* - фильтрация для активностей мишеней.
        *   `standard_relation`: *list[string]* - список соотношений (например, `=`).
        *   `standard_units`: *list[string]* - список единиц измерения.
        *   `target_organism`: *list[string]* - список типов организмов.
        *   `standard_type`: *list[string]* - список типов измерений.
        *   `assay_type`: *list[string]* - список типов биологических тестов.
    *   `cell_lines`: *dictionary* - фильтрация для активностей клеточных линий.
        *   `standard_relation`: *list[string]* - список соотношений (например, `=`).
        *   `standard_units`: *list[string]* - список единиц измерения.
        *   `assay_organism`: *list[string]* - список типов организмов.
        *   `standard_type`: *list[string]* - список типов измерений.
        *   `assay_type`: *list[string]* - список типов биологических тестов.

#### ChEMBL_download_cell_lines

Задача по загрузке клеточных линий с ChEMBL.

*   `download`: *boolean* - логический флаг, указывающий, следует выполнять эту задачу в текущем запуске программы.
*   `logger_label`: *string* - метка, используемая для сообщений журнала, связанных с этой задачей.
*   `logger_color`: *string* - цветовой код для вывода журнала.
*   `results_folder_name`: *string* - имя папки для хранения загруженных данных о клеточных линиях.
*   `results_file_name`: *string* - имя файла для сохранения данных о клеточных линиях.
*   `download_activities`: *boolean* - логический флаг, указывающий, следует ли загружать данные об активности для клеточных линий.
*   `raw_csv_folder_name`: *string* - имя папки для хранения необработанных данных в формате .csv.
*   `raw_csv_g_drive_id`: *string* - идентификатор Google.Drive архива, в котором лежат неочищенные файлы с необходимыми активностями (необходим, так как активности к клеточным линиям через интерфейс `chembl_webresource_client` или API ChEMBL - не вышло).
*   `download_all`: *boolean* - логический флаг, указывающий, следует ли загружать данные для всех клеточных линий или только для клеточных линий, указанных в `id_list`.
*   `download_compounds_sdf`: *boolean* - логический флаг, указывающий, следует ли загружать соединения в формате .sdf.
*   `id_list`: *list[string]* - список ChEMBL_ID для конкретных клеточных линий, для которых необходимо загрузить данные.

#### ChEMBL_download_compounds

Задача по загрузке соединений с ChEMBL.

*   `download`: *boolean* - логический флаг, указывающий, следует выполнять эту задачу в текущем запуске программы.
*   `logger_label`: *string* - метка, используемая для сообщений журнала, связанных с этой задачей.
*   `logger_color`: *string* - цветовой код для вывода журнала.
*   `results_folder_name`: *string* - имя папки для хранения загруженных данных о соединениях.
*   `molfiles_folder_name`: *string* - имя папки для хранения mol- и sdf-файлов соединений.
*   `combined_file_name`: *string* - имя файла для сохранения объединенных данных о соединениях.
*   `need_combining`: *boolean* - логический флаг, указывающий, нужно ли объединять соединения в один файл.
*   `delete_after_combining`: *boolean* - логический флаг, указывающий, следует ли удалять оставшиеся данные после объединения.
*   `mw_ranges`: *list[lists[float]]* - список диапазонов молекулярной массы, используемых для фильтрации загрузки соединений.

#### ChEMBL_download_targets

Задача по загрузке мишеней с ChEMBL.

*   `download`: *boolean* - логический флаг, указывающий, следует выполнять эту задачу в текущем запуске программы.
*   `logger_label`: *string* - метка, используемая для сообщений журнала, связанных с этой задачей.
*   `logger_color`: *string* - цветовой код для вывода журнала.
*   `results_folder_name`: *string* - имя папки для хранения загруженных данных о мишенях.
*   `results_file_name`: *string* - имя файла для сохранения данных о мишенях.
*   `download_activities`: *boolean* - логический флаг, указывающий, следует ли загружать данные об активностях для мишеней.
*   `download_all`: *boolean* - логический флаг, указывающий, следует ли загружать данные для всех мишеней или только для мишеней, указанных в `id_list`.
*   `download_compounds_sdf`: *boolean* - логический флаг, указывающий, следует ли загружать соединения в формате SDF.
*   `id_list`: *list[string]* - список ChEMBL_ID для конкретных мишеней, для которых необходимо загрузить данные.

### PubChem

#### PubChem_download_toxicity

Задача по загрузке токсичности соединений линий с PubChem (ChemIDPlus).

*   `download`: *boolean* - логический флаг, указывающий, следует выполнять эту задачу в текущем запуске программы.
*   `logger_label`: *string* - метка, используемая для сообщений журнала, связанных с этой задачей.
*   `logger_color`: *string* - цветовой код для вывода журнала.
*   `results_folder_name`: *string* - имя папки для хранения загруженных данных о токсичности.
*   `molfiles_folder_name`: *string* - имя папки для хранения mol- и sdf-файлов соединений.
*   `results_file_name`: *string* - имя файла для сохранения данных о токсичности.
*   `combined_file_name`: *string* - имя файла для сохранения объединенных данных о токсичности.
*   `need_combining`: *boolean* - логический флаг, указывающий, нужно ли объединять данные.
*   `delete_after_combining`: *boolean* - логический флаг, указывающий, следует ли удалять оставшиеся данные после объединения.
*   `download_compounds_sdf`: *boolean* - логический флаг, указывающий, следует ли загружать соединения в формате SDF.
*   `filtering`: *dictionary* - словарь, содержащий параметры фильтрации данных о токсичности.
    *   `kg`: *dictionary* - фильтрация для дозы, указанной в `mg/kg`.
        *   `organism`: *list[string]* - список организмов для фильтрации.
        *   `route`: *list[string]* - список способов введения для фильтрации.
        *   `testtype`: *list[string]* - список типов тестирования для фильтрации.
    *   `m3`: *dictionary* - фильтрация для дозы, указанной в `mg/m3`.
        *   `organism`: *list[string]* - список организмов для фильтрации.
        *   `route`: *list[string]* - список способов введения для фильтрации.
        *   `testtype`: *list[string]* - список типов тестирования для фильтрации.
    *   `need_filtering_by_characteristics`: *boolean* - логический флаг, указывающий, следует сохранять отфильтрованные по количеству вхождений характеристик выборки токсичности.
    *   `occurrence_characteristics_number`: *integer* - минимальное количество вхождений характеристик в соотв. выборки.
    *   `characteristics_subfolder_name`: *string* - имя подпапки для хранения отфильтрованных по количеству вхождений характеристик выборок токсичности.
*   `sleep_time`: *float* - число с плавающей точкой, представляющее время ожидания между запросами (в секундах, для предотвращения блокировки со стороны PubChem).
*   `start_page`: *integer* - число, представляющее начальную страницу данных о токсичности.
*   `end_page`: *integer* - число, представляющее последнюю страницу данных о токсичности.
*   `limit`: *integer* - число, представляющее лимит для кол-ва данных за 1 запрос.

### Utils

#### CombineCSVInFolder

*   `logger_label`: *string* - метка, используемая для сообщений журнала, связанных с этой задачей.
*   `logger_color`: *string* - цветовой код для вывода журнала.

#### VerboseLogger

*   `verbose_print`: *boolean* - логический флаг, указывающий, включен ли подробный вывод в консоль.
*   `message_ljust`: *integer* - ширина левого выравнивания для сообщений лога.
*   `exceptions_file`: *string* - имя файла для записи исключений.
*   `output_to_exceptions_file`: *boolean* - логический флаг, указывающий, следует ли выводить весь вывод только в файл для записи исключений.

#### ReTry

*   `attempts_amount`: *integer* - количество попыток по умолчанию.
*   `sleep_time`: *float* - время ожидания между попытками (в секундах) по умолчанию.

## Sources

### Official Python libraries documentation:

  * PubChemPy documentation. — Текст : электронный // Read the Docs : [сайт](https://pubchempy.readthedocs.io/en/latest)
  * Requests: HTTP for Humans™. — Текст : электронный // Read the Docs : [сайт](https://requests.readthedocs.io/en/latest)
  * Loguru. Python loggig made (stupidly) simple. — Текст : электронный // Read the Docs : [сайт](https://loguru.readthedocs.io/en/stable)

<hr></hr>

### ChEMBL sources:

  * A python client for accessing ChEMBL web services / Michal. — Текст : электронный // The ChEMBL-og : [сайт](https://chembl.blogspot.com/2014/05/a-python-client-for-accessing-chembl.html)
  * Explore Chemistry. Quickly find chemical information from authoritative sources. — Текст : электронный // PubChem: An official website of the United States government : [сайт](https://pubchem.ncbi.nlm.nih.gov)
  * ChEMBL is a manually curated database of bioactive molecules with drug-like properties. It brings together chemical, bioactivity and genomic data to aid the translation of genomic information into effective new drugs. — Текст : электронный // ChEMBL : [сайт](https://www.ebi.ac.uk/chembl)

<hr></hr>

### PubChem sources:

  * Welcome to the IUPAC FAIR Chemistry Cookbook / International Union of Pure and Applied Chemistry. — Текст : электронный // GitHub Pages : [сайт](https://iupac.github.io/WFChemCookbook/intro.html)
  * PubChem_SDQ_Bibliometrics / Винсент Ф. Скальфани, Серена С. Ральф, Али Аль Альшейх, Джейсон Э. Бара. — Текст : электронный // GitHub Pages : [сайт](https://vfscalfani.github.io/MATLAB-cheminformatics/live_scripts_html/PubChem_SDQ_Bibliometrics.html)
  * PubChem_SDQ_LitSearch / Винсент Ф. Скальфани, Серена С. Ральф, Али Аль Альшейх, Джейсон Э. Бара. — Текст : электронный // GitHub Pages : [сайт](https://vfscalfani.github.io/MATLAB-cheminformatics/live_scripts_html/PubChem_SDQ_LitSearch.html)

<hr></hr>

### Notebooks submodules (used, but removed from repo):

  * IUPAC WFChemCookbook repository / International Union of Pure and Applied Chemistry. — Текст : электронный // GitHub : [сайт](https://github.com/IUPAC/WFChemCookbook)
  * FionaEBI ChEMBL notebooks repository / FionaEBI. — Текст : электронный // GitHub : [сайт](https://github.com/chembl/notebooks)
