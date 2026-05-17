# Character model — deferred fields and filters

Поля и фильтры платформенной character-модели, которые сознательно отложены и не входят в текущий core. Когда созреет
дизайн каждого блока — он добавляется отдельным слайсом.

## Deferred fields

| Field                                         | Reason for deferral                                                                                                             |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `accessLevel`                                 | Не уверен, что хотим читать прямо из `characters.accesslevel` источника — нужна отдельная политика.                             |
| `lastAccess`                                  | Читать ли из `characters.lastAccess` или derive из online-set transitions — не решено.                                          |
| `onlineTime`                                  | То же; вдобавок vanilla L2J пишет `onlinetime` только при logout, что даёт лаг в реальном времени.                              |
| `hero`                                        | Generic L2-поле, но не критично для первой итерации UI.                                                                         |
| `lockIp`, `lockHwid`, `lockItems`             | Build-specific (bohpts хранит как именованные `character_variables`, vanilla не использует). Нужен сборко-агностичный механизм. |
| `blockedExp`                                  | Build-specific, тот же случай — конкретный ключ в `character_variables`.                                                        |
| Last-login `ip`, `hwid`                       | Требуют моделирования `character_auth_history` или эквивалента; отложено.                                                       |
| `coinsDeposited` (выведено)                   | Не character-domain, считается из shop/transfers сервиса. Cross-service read на платформенной стороне.                          |
| `tgUsers[]`                                   | Не character-domain, держится в nx-users / telegram-link.                                                                       |
| Clan `crestPath`, `allyCrestPath`, `clanName` | Резолвится через Clan entity, не дублируется в Character.                                                                       |

## Deferred filters

| Filter                              | Reason for deferral                                                                                                                               |
|-------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `ip`, `hwid`                        | Нет last-login IP/HWID полей в core, нет character_auth_history.                                                                                  |
| `lock-ip / lock-hwid / lock-items`  | Опираются на build-specific lock-флаги.                                                                                                           |
| `blocked-exp`                       | Опирается на build-specific флаг.                                                                                                                 |
| `unique-auth` (HWID / IP / HWID_IP) | Требует character_auth_history агрегата.                                                                                                          |
| `store-mode` (множественный)        | `privateStore` enum в DTO уже есть, но filter-семантика «несколько режимов одновременно» не специфицирована; добавим, когда фронт явно потребует. |
| `telegram-user`                     | Не character-domain (см. tgUsers выше).                                                                                                           |

## Notes

- Build-specific атрибуты (lock-флаги, blockedEXP) ждут отдельного дизайна — generic key-value extension или
  typed-optional поля. Не моделируем, пока не появится core-agnostic форма.
- Любое поле/фильтр из этого backlog добавляется отдельным design spec'ом, не наскоро в существующий core.
