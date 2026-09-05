# Развертывание Cronos alpha

Приложение разворачивается только в существующем namespace `cronos-bot` контекста
`yc-gradius`. Workflow не создает namespace, не применяет `k8s/infra`, не меняет
ServiceAccount, права доступа, сетевые политики или другие сервисы кластера.

## Состав приложения

| Роль | Команда | CPU request / limit | RAM request / limit |
| --- | --- | --- | --- |
| Gateway | `python -m cronos.gateway` | 50m / 250m | 96 / 256 MiB |
| Worker | `python -m cronos.worker` | 150m / 1000m | 256 / 1536 MiB |
| Coordinator | `python -m cronos.coordinator` | 25m / 200m | 64 / 192 MiB |
| Миграция | `python -m cronos.storage` | 50m / 500m | 128 / 512 MiB |

Worker и coordinator работают в двух контейнерах одного pod. Их общий PVC
`cronos-artifacts` имеет размер 5 GiB, класс `yc-network-hdd` и режим RWO. Стратегия
`Recreate` исключает одновременный запуск старого и нового pod приложения при обновлении.
Gateway тоже обновляется через `Recreate`, чтобы не создавать два Telegram poller.
При обновлении возможна короткая пауза; приложение должно восстанавливать обработку из
сохраненных в PostgreSQL событий и заданий.

Постоянно работающая часть приложения запрашивает 225m CPU / 416 MiB RAM; ее лимиты
составляют 1450m / 1984 MiB. Вместе с существующей инфраструктурой и одним migration Job:
500m CPU / 832 MiB RAM запросов, 3050m CPU / 3520 MiB RAM лимитов. Это укладывается в
квоты namespace 2 CPU / 4 GiB для requests и 4 CPU / 6 GiB для limits. Общий объем PVC
с учетом PostgreSQL и RabbitMQ составляет 11 GiB при квоте 20 GiB.

Все контейнеры запускаются с UID/GID 10001, `fsGroup: 10001`, без token ServiceAccount,
без capabilities и privilege escalation, с `RuntimeDefault` seccomp и read-only root FS.
Для временных файлов выделены ограниченные `emptyDir`. Артефакты переживают перезапуск
pod благодаря PVC; временные файлы из `/tmp` не сохраняются при замене pod.

## Предварительные условия

В `cronos-bot` должны существовать сервисы `postgres`, `rabbitmq`, `redis`,
ServiceAccount `cronos-runtime` и следующие Secrets:

| Secret | Ключи для приложения |
| --- | --- |
| `cronos-infra` | `DATABASE_URL`, `RABBITMQ_URL`, `REDIS_URL` |
| `cronos-infra`, только Job | `ADMIN_DATABASE_URL` |
| `cronos-app` | `TELEGRAM_BOT_TOKEN`, `ALLTOKENS_API_KEY`, `ALLTOKENS_BASE_URL` |
| `image-pull-secret` | существующий `kubernetes.io/dockerconfigjson` |

Runtime выбирает конкретные ключи через `secretKeyRef`; целые Secrets через `envFrom`
не импортируются. Доступ администратора PostgreSQL есть только у migration Job.
Инициализация LangGraph checkpoints выполняется worker при запуске под прикладной ролью.

Для автоматической публикации и развертывания в GitHub Actions нужны репозиторные Secrets:

- `YC_JSON_CREDENTIALS`: JSON авторизованного ключа, имеющего доступ на push в registry
  `crpe8jmbmklfe4l31c68`. Если kubeconfig использует YC exec-аутентификацию, этому же
  аккаунту нужен заранее настроенный доступ к ресурсам приложения в `cronos-bot`.
- `KUBE_CONFIG`: kubeconfig как YAML/JSON или его base64-представление; в нем должен быть
  контекст с именем `yc-gradius` и права на ресурсы приложения внутри `cronos-bot`.

Значения Secrets не включаются в образ, исходники, аргументы команд или журналы workflow.
Временный kubeconfig имеет права `0600` и удаляется после deploy job.
Статический token/certificate из kubeconfig сохраняется. Для конфигурации с
`yc ... create-token` абсолютный путь и локальный профиль YC заменяются короткоживущим
IAM token от `YC_JSON_CREDENTIALS`; установка CLI пользователя на runner не требуется.
Другие exec-плагины не запускаются. Если у CI-аккаунта нет требуемых прав, deploy
завершается ошибкой; workflow не создает права автоматически.

## CI и обновление

Единый workflow `.github/workflows/ci.yml` запускает `uv sync --locked`, Ruff, Ty, pytest и
проверку области манифестов на pull request и push в `main`. Ошибки проверок останавливают
pipeline. На GitHub runner поднимается отдельный PostgreSQL 17 с одноразовыми тестовыми
паролями и раздельными ролями admin/app. Создается принадлежащая приложению схема
`langgraph`; отдельный шаг выполняет миграцию перед проверками RLS/checkpoints на настоящей БД. Эти
реквизиты и база не используются кластером. При локальном запуске без `DATABASE_URL` и
`ADMIN_DATABASE_URL` соответствующие тесты могут быть пропущены.

Успешный push в `main` автоматически собирает Linux amd64 образ:

```text
cr.yandex/crpe8jmbmklfe4l31c68/cronos:<полный SHA коммита>
```

Dockerfile устанавливает зависимости по `uv.lock` с uv 0.8.22, включает `schema.sql`,
а в runtime устанавливает `fonts-dejavu-core` для кириллицы в PDF. Образ один для всех
трех процессов и миграции. Тег `latest` не используется.

Если `YC_JSON_CREDENTIALS` отсутствует, проверки и сборка все равно выполняются.
Workflow экспортирует образ с тем же полным SHA-тегом в `cronos-image.tar` и сохраняет
GitHub Artifact `cronos-image-<полный SHA коммита>` на один день без дополнительного сжатия.
В summary явно указан режим `Artifact only`, выход build job `published=false`, а deploy
job пропускается. Это готовый образ для ручного скачивания и публикации, а не выполненный
deploy. После публикации этого SHA можно запустить `k8s/apps/deploy.sh`, как описано ниже.
Если credentials заданы, но вход в registry или push завершился ошибкой, workflow падает
и не выдает результат за успешную публикацию.

При успешной публикации образа (`published=true`) workflow:

1. Проверяет существование требуемых сервисов и Secrets без чтения их значений в журнал.
2. Проверяет сгенерированные манифесты через server-side dry run.
3. Создает отдельный Job миграции с уникальным именем и ждет его успешного завершения.
4. Применяет только ресурсы из `k8s/apps/kustomization.yaml` с тем же SHA образа.
5. Ждет готовности обоих Deployment. Успешная сборка сама по себе не считается deploy.

Job автоматически удаляется через час после завершения. Если миграция не прошла,
обновление процессов не начинается. Завершенный workflow может быть запущен повторно
через GitHub Actions на `main`; повторный запуск получает новое имя Job. Запуски `main`
сериализованы, поэтому новая версия не прерывает миграцию предыдущей.

При отсутствии `gh` состояние Actions можно прочитать локальным helper:

```sh
uv run python scripts/github_status.py latest
uv run python scripts/github_status.py status RUN_ID
uv run python scripts/github_status.py jobs RUN_ID
uv run python scripts/github_status.py logs RUN_ID --max-lines 50
```

Helper читает GitHub token из настроенного `git credential` helper без вывода значения,
использует только GET и не изменяет Secrets или запуски. `logs` выводит ограниченную
выборку строк ошибок с редактированием credential-шаблонов, а не весь журнал.

Для ручного применения уже опубликованного и проверенного SHA:

```sh
bash k8s/apps/deploy.sh FULL_40_CHARACTER_COMMIT_SHA
```

Скрипт всегда использует `--context yc-gradius --namespace cronos-bot`. Не применяйте
каталог `k8s` целиком: инфраструктура и приложение имеют отдельные жизненные циклы.

## Проверка после запуска

Gateway предоставляет `/healthz` и `/readyz` на внутреннем порту 8000. Доступ снаружи
не опубликован; при необходимости локальной диагностики:

```sh
kubectl --context yc-gradius --namespace cronos-bot port-forward service/cronos-gateway 8000:8000
```

Worker и coordinator должны периодически обновлять `/tmp/worker.healthy` и
`/tmp/coordinator.healthy` в своих контейнерах. Пробы проверяют, что файлу менее 120 секунд;
startup допускает до 180 секунд запуска. Проверка heartbeat должна отражать работу
основного цикла процесса. Готовность pod не заменяет проверку ответа пользователю,
доставки отложенного сообщения после рестарта, provider usage и выдачи файла.

```sh
kubectl --context yc-gradius --namespace cronos-bot get pods,pvc
kubectl --context yc-gradius --namespace cronos-bot logs deployment/cronos-runtime -c worker --tail=100
kubectl --context yc-gradius --namespace cronos-bot logs deployment/cronos-runtime -c coordinator --tail=100
kubectl --context yc-gradius --namespace cronos-bot logs deployment/cronos-gateway --tail=100
```

Для возврата версии используйте ранее проверенный SHA образа после оценки совместимости
с текущей схемой PostgreSQL. Автоматические обратные миграции и удаление PVC не выполняются.
Один pod и один диск не обеспечивают высокую доступность; перед расширением alpha нужны
резервные копии базы и артефактов. Ограничения текущей сетевой изоляции зафиксированы в
`k8s/infra/README.md`: существование NetworkPolicy не означает, что CNI их исполняет.
