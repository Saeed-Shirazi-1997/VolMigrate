# Volmigrate — Architecture Design

ابزار Self-hosted برای انتقال داده از Docker Volumes به Kubernetes PersistentVolumeClaims (PVCs).

- **نسخه:** 1.0
- **وضعیت:** معماری پیشنهادی MVP
- **اصل طراحی:** Streaming-first، بدون ساخت آرشیو کامل واسط روی دیسک
- **Storage Backend:** مستقل از برند؛ Longhorn تنها یکی از گزینههاست.

## 1. اهداف و اصول طراحی

Volmigrate به تیمها کمک میکند Volumeهای Docker را کشف کنند، مقصد Kubernetes را انتخاب کنند، پیشنیازها را بررسی کنند و داده را با ثبت وضعیت، گزارش پیشرفت و اعتبارسنجی به PVC منتقل کنند.

اصول کلیدی:

1. **Streaming-first:** انتقال مستقیم داده بدون ایجاد فایل آرشیو کامل روی دیسک.
2. **Storage-agnostic:** استفاده از PVC و StorageClass بدون وابستگی به یک Storage Provider خاص.
3. **Control Plane / Execution Plane:** جداسازی مدیریت و هماهنگی از اجرای انتقال.
4. **Durable state:** ذخیره پایدار وضعیت Migrationها و رویدادها در PostgreSQL.
5. **Safe by default:** توقف منبع یا حذف PVC مقصد بدون تأیید و سیاست صریح انجام نشود.
6. **Recoverability:** خرابی Worker نباید تاریخچه و وضعیت پایدار Migration را از بین ببرد.
7. **Least privilege:** دسترسی محدود به Docker و Kubernetes و حفاظت از credentialها.

## 2. معماری کلی

```text
User / Operator
      |
      v
React + TypeScript UI
      |
    REST / SSE
      |
      v
FastAPI Control Plane
      |
      +---- Connection Manager
      +---- Discovery & Preflight
      +---- Migration Planner
      +---- Migration Orchestrator
      |
      +---- PostgreSQL
      |        |
      |        +---- Migration State
      |        +---- Events / History
      |        +---- Durable Jobs
      |
      v
Worker Process
      |
      +---- Docker Source Adapter
      |
      +---- Streaming Transfer Engine
      |
      +---- Kubernetes Destination Adapter
                    |
                    v
             Kubernetes API
                    |
                    v
             PVC / StorageClass / CSI
```

در MVP، PostgreSQL میتواند هم persistence و هم صف اولیه Jobها را تأمین کند. در شروع نیازی به Microservice یا Message Broker مستقل نیست.

## 3. فناوریهای پیشنهادی

| لایه | فناوری |
|---|---|
| Backend | Python 3.12+ / FastAPI |
| Frontend | React + TypeScript |
| Persistence | PostgreSQL |
| Database Migration | Alembic |
| Docker Integration | Docker SDK for Python |
| Kubernetes Integration | Kubernetes Python Client |
| Live Progress | Server-Sent Events (SSE) |
| Testing | pytest |
| Initial Deployment | Docker Compose |

این انتخابها نقطه شروع هستند و در صورت نیاز میتوانند تغییر کنند.

## 4. Control Plane

مسئول مدیریت و هماهنگی است، نه انتقال مستقیم دادههای حجیم.

مسئولیتها:

- احراز هویت و کنترل دسترسی
- مدیریت Connectionها
- کشف Volumeها و منابع مقصد
- ساخت Migration Plan
- اجرای Preflight
- ایجاد، زمانبندی و لغو Jobها
- ثبت وضعیت و رویدادها
- ارائه REST API و SSE
- ثبت Audit Trail

## 5. Execution Plane

مسئول اجرای واقعی انتقال داده است.

مسئولیتها:

- دریافت اتمیک Job
- ثبت Lease و Heartbeat
- آمادهسازی Pod یا Job موقت در Kubernetes
- Mount کردن PVC مقصد
- خواندن داده از Docker
- انتقال Stream به مقصد
- مدیریت Backpressure، Timeout و Cancellation
- اعتبارسنجی داده
- پاکسازی منابع موقت
- ثبت نتیجه در PostgreSQL

Worker باید مستقل از چرخه عمر درخواست HTTP باشد؛ بستهشدن تب مرورگر نباید انتقال را متوقف کند.

## 6. ماژولهای نرمافزار

| ماژول | مسئولیت |
|---|---|
| `connections` | مدیریت اتصالهای Docker و Kubernetes |
| `discovery` | کشف Volumeها و منابع مقصد |
| `planning` | ساخت Plan و بررسی سازگاری |
| `migrations` | مدیریت چرخه عمر Migration |
| `transfer` | موتور انتقال داده |
| `sources` | قرارداد و Adapter منبع |
| `destinations` | قرارداد و Adapter مقصد |
| `validation` | بررسی صحت داده |
| `persistence` | مدلها و Repositoryها |
| `security` | مجوزها، Secretها و اعتبارسنجی ورودی |
| `observability` | Log، Metrics و Event |

پیشنهاد MVP یک **Modular Monolith بههمراه Worker مستقل** است.

## 7. Docker Source Adapter

مسئولیتها:

- اتصال به Docker Engine
- کشف و Inspect کردن Volumeها
- شناسایی Driver و Metadata
- نمایش Containerهای مصرفکننده
- بررسی دسترسی واقعی Worker به داده
- ارائه Read Stream به Transfer Engine

محدودیتها:

- Mountpoint گزارششده لزوماً از Worker قابل دسترسی نیست.
- Volumeهای Remote و Plugin-based ممکن است به پشتیبانی اختصاصی نیاز داشته باشند.
- دسترسی به Docker Socket بسیار قدرتمند است؛ نباید آن را بدون حفاظت روی شبکه عمومی در دسترس قرار داد.
- اتصال Remote باید از روش امن مانند SSH یا Docker API با TLS استفاده کند.

قرارداد مفهومی:

```text
SourceAdapter
  ├── test_connection()
  ├── list_volumes()
  ├── inspect_volume(volume_id)
  ├── list_consumers(volume_id)
  ├── check_read_access(volume_id)
  └── open_read_stream(volume_id, options)
```

## 8. Kubernetes Destination Adapter

مسئولیتها:

- اتصال امن به Kubernetes API
- کشف Namespace و StorageClass
- انتخاب PVC موجود یا ایجاد PVC جدید
- بررسی ظرفیت، Access Mode و محدودیتهای Topology
- انتظار برای `Bound` شدن PVC
- آمادهسازی Pod یا Job موقت برای Mount کردن PVC
- فراهمکردن مقصد قابل نوشتن
- پاکسازی منابع موقت

Kubernetes و CSI معمولاً Provisioning، Attach و Mount کردن Storage را مدیریت میکنند؛ محتوای فایلها را از Docker Volume منتقل نمیکنند. انتقال محتوا مسئولیت Volmigrate است.

ملاحظات امنیتی:

- Pod انتقال نباید بهصورت پیشفرض Privileged باشد.
- ServiceAccount باید فقط مجوزهای ضروری را داشته باشد.
- معماری به Longhorn یا Storage Provider خاص وابسته نباشد.

قرارداد مفهومی:

```text
DestinationAdapter
  ├── test_connection()
  ├── list_namespaces()
  ├── list_storage_classes()
  ├── inspect_pvc(namespace, pvc_name)
  ├── prepare_destination(plan)
  ├── wait_until_writable(destination)
  ├── open_write_stream(destination, options)
  └── cleanup_temporary_resources(resources)
```

## 9. Transfer Engine

Transfer Engine باید از Docker و Kubernetes مستقل باشد.

مسیر انتقال:

```text
Docker Volume
      |
      v
Source Reader / tar stream
      |
      v
Bounded Buffer + Backpressure
      |
      v
Destination Writer / Safe Extraction
      |
      v
Mounted PVC
```

### روش پیشنهادی MVP: Streaming tar

داده از طریق Stream یا Pipe منتقل میشود؛ فایل آرشیو کامل روی دیسک ایجاد نمیشود.

الزامات:

- Buffer محدود برای جلوگیری از مصرف بیش از حد حافظه
- Backpressure برای هماهنگی سرعت منبع و مقصد
- تشخیص قطع ارتباط و Stream ناقص
- حفظ Permission، UID/GID، Symlink و فایلهای مخفی
- جلوگیری از Path Traversal و استخراج مسیرهای غیرمجاز
- تعریف سیاست برخورد با فایلهای موجود در PVC
- عدم ثبت موفقیت در صورت خطای مقصد

قابلیتهای آینده:

- Manifest فایلها
- Hash برای اعتبارسنجی
- انتقال Chunk-based
- Resume
- انتقال افزایشی و Sync نهایی

Resume قابلاتکا صرفاً با نگهداشتن Offset یک Stream از نوع tar تضمین نمیشود.

## 10. Consistency و توقف نوشتن

کپی کردن Volume در زمان نوشتن برنامه الزاماً نسخهای سازگار ایجاد نمیکند.

### سیاست MVP: Offline Copy

1. شناسایی مصرفکنندگان Volume
2. نمایش آنها به اپراتور
3. تأیید توقف برنامه یا عدم وجود Write
4. اجرای انتقال
5. اعتبارسنجی داده
6. اتصال مقصد به Workload جدید

بدون تأیید صریح، Container منبع بهصورت خودکار متوقف نشود.

**نکته دیتابیس:** کپی خام فایلهای فعال PostgreSQL روش عمومی و امنی برای مهاجرت دیتابیس نیست. برای دیتابیس باید از Backup/Restore سازگار با دیتابیس یا Workflow اختصاصی استفاده کرد.

قابلیتهای آینده:

- Snapshot سازگار با Backend
- انتقال اولیه و Sync نهایی
- Hookهای Lifecycle
- Workflowهای Application-aware

## 11. وضعیت Migration

```text
Draft
  |
  v
Preflight
  |
  v
Queued
  |
  v
Preparing
  |
  v
Transferring
  |
  v
Validating
  |
  v
Completed
```

حالتهای جانبی:

- `Failed`
- `Cancelled`
- `NeedsAttention`
- `CleanupPending`

قواعد اجرا:

- Claim کردن Job باید اتمیک باشد.
- Worker باید Lease و Heartbeat داشته باشد.
- وضعیت مراحل در PostgreSQL ذخیره شود.
- مراحل تا حد امکان Idempotent باشند.
- Retry نباید کورکورانه روی مقصد نیمهمنتقلشده اجرا شود.
- Worker جدید باید وضعیت مقصد و منابع قبلی را بررسی کند.
- درصد پیشرفت فقط زمانی نمایش داده شود که حجم کل قابل برآورد باشد؛ در غیر این صورت بایت منتقلشده و سرعت نمایش داده شوند.

## 12. مدل داده پیشنهادی

| موجودیت | کاربرد |
|---|---|
| `Connection` | تنظیمات اتصال |
| `Credential Reference` | ارجاع به Secret امن |
| `Migration` | منبع، مقصد، Policy و وضعیت |
| `Migration Event` | تاریخچه تغییر وضعیت و خطاها |
| `Resource Reference` | Volume، Namespace، PVC و منابع موقت |
| `Worker Lease` | مالک Job و آخرین Heartbeat |

Secret خام نباید در Log، Event یا متن Exception ذخیره شود. تنظیمات مؤثر بر Migration باید هنگام شروع Snapshot شوند.

## 13. API و ارتباط زنده

| Method | Endpoint | کاربرد |
|---|---|---|
| GET | `/api/connections` | فهرست اتصالها |
| POST | `/api/connections` | ثبت اتصال |
| POST | `/api/connections/{id}/test` | تست اتصال |
| GET | `/api/sources/{id}/volumes` | کشف Volume |
| GET | `/api/destinations/{id}/storage-classes` | فهرست StorageClass |
| POST | `/api/migrations/plan` | ساخت Plan و Preflight |
| POST | `/api/migrations` | ایجاد Migration |
| GET | `/api/migrations/{id}` | وضعیت Migration |
| POST | `/api/migrations/{id}/cancel` | درخواست لغو |
| POST | `/api/migrations/{id}/retry` | درخواست Retry مجاز |
| GET | `/api/migrations/{id}/events` | رویدادهای SSE |

داده حجیم نباید از API یا UI عبور کند. API مسئول کنترل و Metadata است و Execution Plane داده را منتقل میکند.

## 14. رابط کاربری

صفحات اصلی:

1. **Dashboard:** وضعیت Migrationهای جاری، شکستخورده و تکمیلشده
2. **Connections:** تعریف و تست اتصالها
3. **Volume Explorer:** نمایش Volumeها و مصرفکنندگان
4. **Migration Wizard:** انتخاب منبع، PVC، Namespace، StorageClass، ظرفیت و Policy
5. **Migration Details:** مرحله، سرعت، رویدادها، خطاها، اعتبارسنجی و Cancel/Retry
6. **Settings / Audit:** تنظیمات دسترسی و تاریخچه عملیات

## 15. امنیت

- TLS و احراز هویت برای API
- کنترل دسترسی به Connection و Migration
- عدم Exposure عمومی Docker Socket
- Kubernetes RBAC با حداقل مجوز
- عدم استفاده پیشفرض از Podهای Privileged
- حذف Secretها از Log و Event
- محافظت از Endpointهای قابل تنظیم در برابر SSRF
- استخراج امن Archive
- تأیید صریح برای عملیات مخرب
- نگهداری Audit Trail

فرض MVP یک نصب Self-hosted برای یک سازمان است. Multi-tenancy مشترک نیازمند طراحی جداگانه برای جداسازی Tenantها و Credentialهاست.

## 16. خطا، Retry و Cleanup

خطاهای مهم:

- عدم دسترسی به Docker یا Kubernetes
- Driver پشتیبانینشده
- عدم دسترسی به Mountpoint
- PVC که به Bound نمیرسد
- ظرفیت ناکافی
- مقصد غیرقابلنوشتن
- قطع Stream
- خرابی Worker
- شکست Validation
- Cleanup ناقص

قواعد:

- خطاها به پیام قابل اقدام تبدیل شوند.
- Retry فقط برای خطاهای قابل تکرار و پس از بررسی مقصد انجام شود.
- Cleanup باید Idempotent باشد.
- **PVC مقصد در شکست یا لغو بهصورت پیشفرض حذف نشود.**
- حذف منابع موقت از حذف داده اصلی تفکیک شود.

## 17. استقرار MVP

اجزای اولیه:

- Web/API
- یک یا چند Worker
- PostgreSQL
- اتصال امن به Docker Engine
- اتصال امن به Kubernetes API

Worker میتواند ابتدا کنار Control Plane اجرا شود. در آینده میتوان آن را نزدیک منبع یا داخل کلاستر مقصد اجرا کرد؛ این تصمیم روی شبکه، دسترسی Docker و RBAC اثر میگذارد.

## 18. Validation و Observability

### Validation

1. اطمینان از تکمیل Stream
2. بررسی ساختار فایلها و Metadata
3. مقایسه Manifest و اندازه فایلها
4. مقایسه Hash در صورت انتخاب Policy قویتر
5. بررسی قابلیت Mount و Write روی PVC
6. اجرای Smoke Test برنامه در صورت وجود Workflow اختصاصی

Hash کردن تمام فایلها میتواند زمان و I/O زیادی مصرف کند؛ بهتر است قابل تنظیم باشد.

### Observability

- Log ساختاریافته با `migration_id` و `worker_id`
- Eventهای مرحلهای در PostgreSQL
- Metrics برای Throughput، Duration، Retry، Error و Queue Age
- نمایش حجم منتقلشده و نرخ انتقال
- عدم ثبت Secret یا داده حساس
- Correlation ID بین API و Worker

## 19. محدوده MVP

### داخل MVP

- Self-hosted برای یک سازمان
- Docker Engine بهعنوان منبع
- Kubernetes PVC بهعنوان مقصد
- استقلال از برند Storage
- Streaming tar بدون آرشیو کامل واسط
- Offline Copy
- Preflight
- گزارش پیشرفت زنده
- Validation پایه
- Cancel و Retry محدود
- Recovery کنترلشده

### خارج از MVP

- Live Migration تضمینشده برای هر برنامه
- کپی خام فایلهای دیتابیس فعال
- Resume در هر نقطه
- Sync دائمی
- پشتیبانی از تمام Docker Volume Pluginها
- مدیریت کامل Workload مقصد
- Multi-tenancy مشترک
- انتقال کامل Compose Stack، مگر با تصمیم صریح

## 20. تصمیمهای باز

1. PVC مقصد: موجود، جدید یا هر دو؟
2. یک Volume در هر Job یا چند Volume از یک Compose Project؟
3. اتصال Docker از طریق Socket، SSH یا API امن؟
4. محل Worker: کنار API، روی میزبان منبع یا داخل کلاستر؟
5. کدام Volume Driverها در MVP پشتیبانی میشوند؟
6. Validation پیشفرض: Manifest یا Hash کامل؟
7. سیاست Retry برای مقصد نیمهکاره چیست؟
8. کدام منابع هرگز نباید خودکار حذف شوند؟
9. آیا Rate Limit و بازه زمانی انتقال لازم است؟
10. آیا Offline Copy کافی است یا Hookهای Lifecycle لازماند؟

## 21. ترتیب پیشنهادی پیادهسازی

1. نهاییسازی قرارداد Source Adapter و Destination Adapter
2. مدل داده و State Machine پایدار
3. Docker Discovery و Kubernetes Preflight
4. Worker، Claim، Lease و Heartbeat
5. Streaming Pipeline با Buffer محدود
6. استخراج امن و Validation پایه
7. SSE و UI وضعیت Migration
8. تست Integration با Docker و Kubernetes واقعی
9. تست خرابی Worker، قطع شبکه، PVC ناموفق، Cancel و Retry
10. مستندسازی نصب، RBAC، محدودیتها و Recovery
