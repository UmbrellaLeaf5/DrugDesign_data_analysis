# Contributing

**Структура сообщения коммита:**

type(scope): message

## Commit subject: `type(scope): message`

*   **type** - описывает цель коммита; должен быть одним из следующих:
    *   `feat`: новая функциональность для пользователя.
    *   `fix`: исправление ошибки.
    *   `docs`: изменения в документации.
    *   `style`: изменения стиля кода (например, форматирование).
    *   `refactor`: рефакторинг кода, который не меняет его поведение.
    *   `test`: добавление или изменение тестов.
    *   `chore`: рутинные задачи, обслуживание или изменения в инструментах.

*   **scope** (optional) - Указывает модуль, компонент или функциональную область, на которую влияет коммит.  Примеры:
    *   `data_analysis`
    *   ...

*   **message** - краткое, повелительное утверждение, обобщающее изменение. Используйте настоящее время и императив английского языка.  Например, "create VerboseLogger".
