# aTSBot Android

Ứng dụng Android chạy bot trực tiếp, không cần Web UI hoặc VPS. Project dùng Java cho giao diện/service và Chaquopy để đóng gói Python bot core.

## Yêu cầu build

- JDK 17
- Android SDK 35
- Android Studio hoặc Gradle Wrapper đi kèm
- Android 8.0 trở lên khi cài APK

## Build

```bash
./gradlew :app:assembleDebug
```

APK debug nằm tại `app/build/outputs/apk/debug/app-debug.apk`.

## Luồng test

1. Chọn server, chế độ, map và điểm farm.
2. Nhập tối đa 5 tài khoản rồi bấm **Đăng nhập**.
3. Chờ login hoàn tất. Ứng dụng tự thử tải pet/skill sau 10 và 20 giây.
4. Có thể bấm **Lấy pet & skill sau login** ở từng slot.
5. Chọn skill nhân vật/pet từ dropdown rồi bấm **Start farm**.

Không commit tài khoản, mật khẩu, cache skill, keystore hoặc capture mạng lên Git.
