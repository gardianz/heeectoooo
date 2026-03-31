# Hecto Bot

Bot browser automation untuk `https://app.hecto.finance/` dengan flow:

- login Hecto
- reconnect / verify Supanova
- ambil OTP dari Gmail via App Password
- sign challenge
- baca state allocate
- unlock semua lock aktif jika ada
- lock ke company dengan `1D` terbaik

Bot ini mendukung:

- multi-account
- scheduler harian
- hanya jalan Senin-Jumat
- timezone `Asia/Jakarta`
- mode lock `value` atau `max`
- minimal lock `5000 HECTO`

## Cara Kerja

Setiap cycle bot akan:

1. login atau restore session
2. baca `HECTO unlocked balance`
3. baca `active locked company` dari API locks
4. jika ada lock aktif dan `unlockAllBeforeLock = true`, unlock semua dulu
5. tunggu sampai lock aktif benar-benar habis
6. cari company dengan `1D` terbaik
7. hitung jumlah HECTO yang akan di-lock
8. lock ke company terbaik

Sumber state yang dipakai:

- unlocked balance: `api.supanova.app/canton/api/balances`
- active lock per company: `/api/locks?userPartyId=...`
- best company by `1D`: gabungan `/api/allocator/companies` + `/api/prices/latest`

## Requirement

- Node.js 20+ atau lebih baru
- Google Chrome terpasang
- akun Hecto + Supanova yang valid
- Gmail App Password untuk akun email yang menerima OTP

## Install

```bash
npm install
```

## File Penting

- [src/index.js](./src/index.js)
- [config.json](./config.json)
- [config.example.json](./config.example.json)
- [accounts.example.json](./accounts.example.json)

## Konfigurasi

### 1. Siapkan `accounts.json`

Gunakan [accounts.example.json](./accounts.example.json) sebagai template untuk membuat `accounts.json`.

Contoh:

```json
[
  {
    "name": "account-1",
    "enabled": true,
    "email": "your-email-1@gmail.com",
    "password": "your-password",
    "gmailAppPassword": "your gmail app password",
    "profileName": "account-1",
    "locking": {
      "amountMode": "max",
      "amountMax": 0
    }
  },
  {
    "name": "account-2",
    "enabled": true,
    "email": "your-email-2@gmail.com",
    "password": "your-password",
    "gmailAppPassword": "your gmail app password",
    "profileName": "account-2",
    "locking": {
      "amountMode": "value",
      "amountValue": 5000,
      "amountMax": 15000
    }
  }
]
```

Field:

- `name`: label akun untuk log
- `enabled`: `true` / `false`
- `email`: email login Hecto
- `password`: password login Hecto
- `gmailAppPassword`: app password Gmail untuk ambil OTP
- `profileName`: folder profile browser persisten per akun
- `locking`: override config lock per akun

### 2. Atur `config.json`

Contoh default:

```json
{
  "timezone": "Asia/Jakarta",
  "schedule": {
    "enabled": true,
    "hour": 4,
    "minute": 30,
    "weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday"],
    "pollIntervalMs": 30000,
    "runOnStart": false,
    "dedupeAcrossRestarts": true
  },
  "execution": {
    "headless": false,
    "execute": true,
    "closeBrowser": true,
    "browserChannel": "chrome",
    "accountDelayMs": 5000,
    "accountLockTtlMs": 21600000,
    "unlockAfterLock": false,
    "unlockAllOnly": false
  },
  "locking": {
    "minAmount": 5000,
    "amountMode": "max",
    "amountValue": 5000,
    "amountMax": 0,
    "unlockAllBeforeLock": true
  }
}
```

Penjelasan:

- `timezone`: timezone scheduler
- `schedule.enabled`: aktif/nonaktif scheduler
- `schedule.hour`: jam eksekusi
- `schedule.minute`: menit eksekusi
- `schedule.weekdays`: hari aktif
- `schedule.pollIntervalMs`: interval cek scheduler
- `schedule.runOnStart`: kalau `true`, jalankan cycle langsung saat bot start
- `schedule.dedupeAcrossRestarts`: cegah run ganda pada hari yang sama walau proses restart dekat jam trigger
- `execution.headless`: `false` lebih aman untuk debug flow sign
- `execution.execute`: kalau `false`, bot hanya baca state
- `execution.closeBrowser`: tutup browser setelah tiap akun selesai
- `execution.browserChannel`: default `chrome`
- `execution.accountDelayMs`: delay antar akun
- `execution.accountLockTtlMs`: masa berlaku lockfile per akun sebelum dianggap stale
- `execution.unlockAfterLock`: mode test, lock lalu unlock lagi
- `execution.unlockAllOnly`: hanya unlock semua, tanpa lock baru
- `locking.minAmount`: batas minimum lock, jangan isi di bawah `5000`
- `locking.unlockAllBeforeLock`: kalau `true`, unlock semua dulu sebelum lock ulang

## Mode Lock

### `amountMode = "max"`

Bot akan:

1. unlock semua lock aktif
2. cek total `HECTO unlocked` setelah unlock selesai
3. lock full sekaligus dalam satu aksi

Contoh:

```json
"locking": {
  "amountMode": "max",
  "amountMax": 0
}
```

Arti `amountMax`:

- `0`: tidak dibatasi, pakai seluruh saldo unlocked
- `> 0`: pakai saldo unlocked, tapi maksimal sebesar `amountMax`

### `amountMode = "value"`

Bot lock sejumlah tetap.

Contoh:

```json
"locking": {
  "amountMode": "value",
  "amountValue": 5000,
  "amountMax": 15000
}
```

Arti:

- target nominal tetap = `amountValue`
- kalau `amountMax > 0`, target akan dibatasi oleh `amountMax`

Catatan:

- lock di bawah `5000` akan di-skip
- untuk `amountMode = "max"`, `amountValue` diabaikan

## Menjalankan Bot

### Scheduler normal

```bash
npm start
```

Mode ini akan standby dan hanya menjalankan cycle pada:

- `04:30 WIB`
- `Senin`
- `Selasa`
- `Rabu`
- `Kamis`
- `Jumat`

Sabtu dan Minggu tidak akan jalan.

### Jalankan sekali sekarang

Set `runOnStart` menjadi `true` di [config.json](./config.json), lalu jalankan:

```bash
npm start
```

Kalau tidak ingin scheduler loop terus, set:

```json
"schedule": {
  "enabled": false
}
```

Maka bot akan menjalankan satu cycle saja lalu selesai.

### Read-only

Di [config.json](./config.json):

```json
"execution": {
  "execute": false
}
```

Bot hanya login dan baca state tanpa unlock/lock.

### Unlock semua saja

Di [config.json](./config.json):

```json
"execution": {
  "unlockAllOnly": true
}
```

Bot hanya unlock semua active lock, tidak lock ulang.

### Test lock lalu unlock

Di [config.json](./config.json):

```json
"execution": {
  "unlockAfterLock": true
}
```

Bot lock dulu, lalu unlock lagi setelah lock terverifikasi aktif.

## Multi-Account

Bot membaca semua akun aktif dari `accounts.json` lalu memprosesnya satu per satu.

Setiap akun memiliki:

- profile browser sendiri di folder `.profile/<profileName>`
- pengaturan lock sendiri
- sesi login sendiri
- lockfile sendiri di `.runtime/account-locks/`

Ini sengaja sequential supaya flow:

- OTP
- session restore
- sign message
- sign & send

tetap stabil dan tidak saling bentrok.

## Folder Runtime

- `.profile/`
  Menyimpan session browser persisten per akun
- `.runtime/`
  Menyimpan state scheduler seperti penanda cycle harian terakhir
  dan lockfile per akun agar dua proses tidak mengeksekusi akun yang sama bersamaan
- `output/`
  Menyimpan screenshot dan log runtime

## Keamanan

File berikut jangan di-commit:

- `accounts.json`
- `.profile/`
- `.runtime/`
- `output/`

Folder ini sudah di-ignore di [.gitignore](./.gitignore).

## Catatan Operasional

- Jika session habis, bot bisa recover lewat challenge `Sign Message` di `/auth`
- Jika masih butuh email/password + OTP, bot akan isi form dan ambil OTP dari Gmail
- Jika website berubah struktur DOM atau flow Privy/Supanova, selector mungkin perlu disesuaikan
- Untuk VPS, jalankan dengan Chrome yang tersedia dan pertimbangkan mode headless hanya setelah flow stabil

## Menjalankan di VPS Ubuntu

Rekomendasi minimum:

- Ubuntu 22.04 atau lebih baru
- Google Chrome atau Chromium terpasang
- Node.js 20+
- timezone server tetap boleh UTC, karena bot menghitung schedule berdasarkan `timezone` di config

Alur yang disarankan:

1. install dependency dengan `npm install`
2. siapkan `accounts.json` dan `config.json`
3. pastikan akun bisa login sekali dalam mode non-headless
4. setelah stabil, jalankan via process manager seperti `pm2` atau `systemd`

Catatan:

- profile browser persisten disimpan di `.profile/`
- kalau Chrome di VPS berbeda channel, sesuaikan `execution.browserChannel`
- untuk VPS tanpa desktop/X server, set `execution.headless: true` atau jalankan dengan `xvfb-run`
- bila mode headless bermasalah pada flow Privy/Supanova, gunakan headful + virtual display

Contoh cepat di VPS:

```bash
npm start
```

Jika `config.json` masih `headless: false`, bot sekarang akan otomatis memaksa headless saat mendeteksi Linux tanpa `DISPLAY` atau `WAYLAND_DISPLAY`.

## Audit Singkat

Temuan penting yang sudah diperbaiki:

- source lock aktif dipindah dari `allocator.table.userLocked` ke `/api/locks`
- unlock batch sekarang mendukung tombol seperti `UNLOCK 5.000`
- sesudah unlock submit, bot menunggu state turun dulu sebelum lanjut
- auth recovery di `/auth` sekarang bisa langsung handle `Sign Message`

Risiko yang masih perlu kamu sadari:

- bot tetap menyimpan kredensial plaintext di `accounts.json`
- profile browser persisten berisi session aktif
- Gmail App Password memberi akses baca OTP inbox
- tanpa state scheduler persisten, restart dekat jam trigger bisa memicu run ganda; bot ini sekarang menyimpan guard harian di `.runtime/schedule-state.json`
- tanpa lockfile per akun, dua proses bot bisa mengeksekusi akun yang sama secara bersamaan; bot ini sekarang memakai `.runtime/account-locks/*.lock`

Karena itu:

- batasi akses folder bot
- jangan commit file rahasia
- gunakan akun khusus bot bila memungkinkan
