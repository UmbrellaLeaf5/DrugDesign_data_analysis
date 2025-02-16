# DrugDesign_data_analysis

## Description

Этот модуль отвечает за загрузку и предварительную обработку данных из [ChEMBL](#sources) и [PubChem](#sources), необходимых для дальнейшего моделирования и анализа при разработке лекарств.

Цель модуля - собрать и подготовить данные о соединениях, их активностях, мишенях и токсичности для последующего использования в задачах машинного обучения.

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

## Configurations

Файл [`configurations.json`](./configurations.json) содержит параметры конфигурации для загрузки и обработки данных, в основном сфокусированного на базах данных `ChEMBL` и `PubChem`. Он определяет настройки для загрузки соединений, активностей, клеточных линий и информации о мишенях из `ChEMBL`, а также данных о токсичности из `PubChem`.

*   **Общие настройки:**
    *   `print_to_console_verbosely`: логический флаг, который, когда установлен в `true`, включает подробный вывод в консоль во время выполнения.
    *   `need_primary_analysis`: логический флаг, указывающий, требуется ли первичный анализ данных.
    *   `primary_analysis_folder_name`: имя папки для хранения результатов первичного анализа.
    *   `testing_flag`: логический флаг, используемый для целей тестирования (при его включении вывод в консоль становится подробнее, а кол-во скачиваемых данных фиксированно и мало)
    *   `skip_downloaded`: логический флаг, указывающий, следует пропускать загрузку, если файлы уже существуют.

*   **ChEMBL_download_activities:** конфигурация для загрузки данных об активности (`IC50`, `Ki`) из `ChEMBL`.
    *   `logger_label`: метка, используемая для сообщений журнала, связанных с этой задачей.
    *   `logger_color`: цветовой код для вывода журнала.
    *   `results_folder_name`: имя папки для хранения загруженных данных об активности.
    *   `download_compounds_sdf`: логический флаг, указывающий, следует ли догружать соединения в формате SDF.

*   **ChEMBL_download_cell_lines:** конфигурация для загрузки данных о клеточных линиях из `ChEMBL`.
    *   `logger_label`: метка, используемая для сообщений журнала, связанных с этой задачей.
    *   `logger_color`: цветовой код для вывода журнала.
    *   `results_folder_name`: имя папки для хранения загруженных данных о клеточных линиях.
    *   `results_file_name`: имя файла для сохранения данных о клеточных линиях.
    *   `download_activities`: логический флаг, указывающий, следует ли загружать данные об активности для клеточных линий.
    *   `raw_csv_folder_name`: имя папки для хранения необработанных данных в формате .csv.
    *   `raw_csv_g_drive_id`: идентификатор Google.Drive для CSV-файла, используемого в процессе.
    *   `download_all`: логический флаг, указывающий, следует ли загружать данные для всех клеточных линий или только для клеточных линий, указанных в `id_list`.
    *   `download_compounds_sdf`: логический флаг, указывающий, следует ли загружать соединения в формате .sdf.
    *   `id_list`: список ChEMBL_ID для конкретных клеточных линий, для которых необходимо загрузить данные.

*   **ChEMBL_download_compounds:** конфигурация для загрузки данных о соединениях из `ChEMBL`.
    *   `logger_label`: метка, используемая для сообщений журнала, связанных с этой задачей.
    *   `logger_color`: цветовой код для вывода журнала.
    *   `results_folder_name`: имя папки для хранения загруженных данных о соединениях.
    *   `molfiles_folder_name`: имя папки для хранения загруженных mol-файлов.
    *   `need_combining`: логический флаг, указывающий, нужно ли объединять соединения в один файл.
    *   `delete_after_combining`: логический флаг, указывающий, следует ли удалять данные после объединения.
    *   `combined_file_name`: имя файла для сохранения объединенных данных о соединениях.
    *   `mw_ranges`: список диапазонов молекулярной массы, используемых для фильтрации загрузки соединений.

*   **ChEMBL_download_targets:** конфигурация для загрузки данных о мишенях из `ChEMBL`.
    *   `logger_label`: метка, используемая для сообщений журнала, связанных с этой задачей.
    *   `logger_color`: цветовой код для вывода журнала.
    *   `results_folder_name`: имя папки для хранения загруженных данных о мишенях.
    *   `results_file_name`: имя файла для сохранения данных о мишенях.
    *   `download_activities`: логический флаг, указывающий, следует ли загружать данные об активности.
    *   `download_all`: логический флаг, указывающий, следует ли загружать данные для всех мишеней или только для мишеней, указанных в `id_list`.
    *   `download_compounds_sdf`: логический флаг, указывающий, следует ли загружать соединения в формате SDF.
    *   `id_list`: список ChEMBL_ID для конкретных мишеней, для которых необходимо загрузить данные.

*   **PubChem_download_toxicity:** конфигурация для загрузки данных о токсичности из `PubChem`.
    *   `logger_label`: метка, используемая для сообщений журнала, связанных с этой задачей.
    *   `logger_color`: цветовой код для вывода журнала.
    *   `results_folder_name`: имя папки для хранения загруженных данных о токсичности.
    *   `sleep_time`: число с плавающей точкой, представляющее время ожидания между запросами (в секундах).
    *   `start_page`: целое число, представляющее начальную страницу данных о токсичности.
    *   `end_page`: целое число, представляющее последнюю страницу данных о токсичности.
    *   `limit`: целое число, представляющее лимит для данных.

Флаг `testing_flag` следует отключить (`false`) для обычного использования.

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

### Notebooks submodules:

  * IUPAC WFChemCookbook repository / International Union of Pure and Applied Chemistry. — Текст : электронный // GitHub : [сайт](https://github.com/IUPAC/WFChemCookbook)
  * FionaEBI ChEMBL notebooks repository / FionaEBI. — Текст : электронный // GitHub : [сайт](https://github.com/chembl/notebooks)

