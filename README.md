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


## Release Note
1. Tổng thể app
- App Android kết nối trực tiếp server game qua phần bot Python chạy trong app.
- Quản lý tối đa 5 account: mỗi account một tab, tab cuối là Điều Khiển.
- Mỗi account có kết nối và trạng thái riêng.
- ACC1 mặc định làm leader, có thể đổi leader khi đang online.
- Bot chạy bằng dịch vụ nền; không cần Windows/VPS riêng. Tuy nhiên Android vẫn có thể dừng app nếu bị hạn chế chạy nền hoặc force stop.
2. Đăng nhập và đăng xuất
Login từng account
Nhập tài khoản → LOGIN → kết nối server → nhận thông tin → đứng chờ lệnh
- Bấm tab nào thì đăng nhập account tab đó.
- Login không tự chạy farm hoặc daily.
- Đã online thì nút Login bị khóa.
- Có checkbox lưu tài khoản/mật khẩu trên máy.
- Mật khẩu lưu cục bộ, hiện chưa có cơ chế mã hóa chuyên biệt.
Login All
- Lấy tài khoản/mật khẩu đã nhập trong cả 5 tab.
- Account đang online được bỏ qua, không đăng nhập lại.
- Kết nối các account chưa chạy.
- Có kiểm tra thiếu mật khẩu và trùng tài khoản.
Out từng account
Yêu cầu Out → xử lý rời party/về SAFE → logout
- Chỉ dừng account được chọn.
- Leader Out không chủ động logout toàn team.
- SAFE là điểm an toàn trong dữ liệu map, không mặc định luôn là phù về thành.
Logout All
- Gửi yêu cầu về SAFE và logout cho toàn bộ account đang chạy.
- Gửi lệnh member trước, leader sau.
- Hiện chưa phải hàng đợi bảo đảm tất cả member logout xong rồi leader mới bắt đầu logout.
3. Thông tin từng account
Dưới Login/Out hiển thị:
- Thành gần nhất.
- Tên khu vực/map hiện tại.
- Tọa độ X/Y và phân khu.
- Bãi train, tọa độ farm.
- Số thành viên party hiện tại so với số dự kiến.
Các mục được gom thành lưới icon:
Mục    Nội dung
Thông tin    Chỉ số nhân vật, pet, tiền, EXP, hiệu suất, tiến độ daily
Rương đồ    Danh sách vật phẩm, số lượng, ô chứa
Nhật ký    EXP, nhặt đồ, dùng vật phẩm và hoạt động
Pet & Skill    Pet ra trận, skill, rule đánh, hồi HP/SP
Dịch chuyển    Những thành đã mở
Bản đồ    Chỉ hiện cho leader


“Rương đồ” hiện đang hiển thị túi đồ được server gửi, không phải chức năng mở kho ngân hàng riêng.
4. Pet & Skill
Lưu riêng theo từng account:
- Pet được chọn ra trận.
- Skill nhân vật.
- Skill pet.
- Rule số quái cho nhân vật và pet, thiết lập độc lập.
- Ngưỡng dùng vật phẩm HP và SP.
- Bật/tắt Phúc Thần.
- Bật/tắt tự ăn Đại Phúc Thần.
Ví dụ rule:
Có từ 4 quái → dùng skill đã chọn
Ít hơn 4 quái → đánh thường
Flow áp dụng:
Chỉnh thiết lập → ÁP DỤNG → lưu theo username → combat dùng thiết lập đó
Lưu ý:
- Hiện hai thanh ngưỡng HP/SP dùng chung giá trị cho cả nhân vật và pet, chưa có 4 thanh riêng.
- Màn hình Pet & Skill được hạn chế refresh khi đang chỉnh để tránh reset dropdown/thanh kéo.
- Nhật ký có ghi sử dụng Phúc Thần và lượt còn lại khi nhận được dữ liệu.
5. Bắt đầu farm trong Điều Khiển
Chọn:
- Chế độ.
- Map train.
- Tọa độ farm.
- Phân khu manual trong tab leader.
Sau đó bấm Bắt đầu farm.
Chế độ Train theo map
Flow chính:
1. Kiểm tra leader online.
2. Chốt danh sách account đang online lúc bấm.
3. Nếu cả đội đã ở map train phù hợp: ưu tiên lập đủ party và đi tới tọa độ, không bắt buộc phù về thành lại.
4. Nếu cần tập trung: phù về thành gần bãi train.
5. Đồng bộ phân khu manual.
6. Leader mời party, member nhận lời.
7. Chọn người INT cao nhất làm quân sư.
8. Leader dẫn party tới điểm farm bằng Ground path.
9. Bật tự đánh tại điểm farm.
Account còn đang connecting không được tính vào danh sách online đã chốt lúc bắt đầu.
Điểm khác với yêu cầu trước của fen: đoạn điều phối hiện vẫn có lúc tắt tự đánh và dùng chế độ chạy trốn khi di chuyển; chưa phải luôn bật combat xuyên suốt đường ra bãi.
Chưa chọn đủ map/tọa độ
- Luồng backend có phương án đưa team về thành gần leader, lập party và đứng chờ.
- Không tự chọn đại một bãi để farm.
Chế độ Đứng yên
- Login và đứng chờ.
- Bấm Bắt đầu farm trong chế độ này không chạy luồng train.
Chế độ Dị giới + farm
Bắt đầu → chạy Dị giới → chờ các account hoàn tất → chuyển sang farm map/tọa độ đã chọn
- Dùng luồng Dị giới có sẵn của bot.
- Không tạo kết nối mới chỉ để đổi chế độ.
- Chưa có giao diện chọn đầy đủ mọi thiết lập Dị giới.
- Luồng hoàn chỉnh này chưa được xác nhận ổn hết bằng test online.
6. Phân khu farm
- UI hiện chọn manual, không có checkbox tự chọn phân khu vắng.
- Danh sách phân khu lấy theo map leader đang đứng.
- Có làm mới khi đổi map và theo chu kỳ khoảng 5 phút.
- Nếu server chưa trả danh sách, UI chưa thể hiện đủ phân khu.
Flow áp dụng:
Về SAFE → giải tán party → chuyển phân khu → lập lại party → tiếp tục mục tiêu farm
Không chủ động logout chỉ vì người dùng yêu cầu đổi phân khu; nhưng lỗi kết nối hoặc nhánh phục hồi vẫn có thể dẫn tới reconnect.
7. Bản đồ leader
Hiển thị:
- Leader và member cùng map/phân khu.
- Quái/NPC, người chơi xung quanh theo dữ liệu nhận được.
- Tên đối tượng.
- Điểm SAFE, điểm farm và đường di chuyển.
Bấm điểm trên map:
Chọn tọa độ → tìm Ground path → kiểm tra đường → gửi lệnh di chuyển leader/team
- Không cố đi thẳng xuyên vật cản.
- Nếu thiếu dữ liệu hoặc đường bất thường, có thể từ chối lệnh.
- Bản đồ đang mở cập nhật khoảng 1 giây/lần.
- Giữ camera ổn định hơn, không liên tục co giãn theo khoảng cách cả đội.
- Member ưu tiên vị trí quan sát mới từ server; nếu không có packet mới thì có thể vẫn hiện vị trí cache.
Đây là bản đồ theo dữ liệu bot, không phải màn hình game đầy đủ.
8. Daily Quest
Chỉ bắt đầu khi bấm Chạy các daily đã tick. Tick checkbox không tự chạy.
Thứ tự xử lý:
1. Phó bản đội đã chọn.
2. Boss quân đoàn.
3. Phó bản đơn.
4. Boss thế giới.
Phó bản đội
Có lựa chọn:
- Cấp 20: Thảo Phạt Thiên Sư.
- Cấp 50: Ngày Tàn Hoạn Quan.
- Cấp 80: Đại Chiến Lữ Bố.
- Cấp 110: Hỏa Thiêu Bộc Dương — mặc định tắt.
Flow:
Chờ xong combat → rời party farm → phù Trác Quận → chờ đủ người → leader tạo phòng → mời bot → member vào/ready → chạy phó bản
- Không bắt buộc cùng phân khu ngoài thành.
- Không bắt buộc lập party farm trước.
- Vào phó bản theo cơ chế phòng.
- Kiểm tra cấp và lượt của các thành viên.
- Nếu một thành viên không đủ điều kiện, luồng có thể bỏ qua phó bản đó.
- Nếu phó bản đội lỗi trong luồng daily thủ công, hiện có cơ chế dừng toàn luồng daily và giữ account online, thay vì cứ reconnect thử lại.
Boss quân đoàn
Kiểm tra còn lượt + hết cooldown → gửi yêu cầu mở → chờ server xác nhận → vào đánh
- ACK 1 hiện được coi là cho phép, 0 là không cho phép.
- Đã xác nhận online ACK 1 dẫn tới trận đánh.
- ACK 0 chưa được xác nhận bằng lần mở thực tế sau cooldown.
- Không đủ điều kiện thì bỏ qua, không ép đánh.
- Có nhánh mở được nhưng không vào combat vẫn gọi reconnect.
Phó bản đơn
- Dùng luồng đánh và mua thêm lượt sẵn có trong bot.
- Chạy theo điều kiện lượt/giới hạn của luồng đó.
- Chưa có UI riêng để đặt ngân sách mua thêm lượt.
Boss thế giới
- Chạy các lượt còn lại.
- Có xử lý vé boss thế giới trong luồng bot.
- Tiến độ phụ thuộc dữ liệu server đã đồng bộ.
Dừng Daily
Bấm Dừng → chờ hết trận đang đánh → không chạy bước kế tiếp → đứng chờ
- Không chủ động logout toàn team.
- Không tự tiếp tục farm sau khi dừng.
- Daily cũng chưa mặc định là “xong rồi tự quay về farm”.
Dù boss/solo đánh riêng từng account, code hiện vẫn có điểm chờ các account hoàn thành một bước rồi mới chuyển bước tiếp theo.
9. Đổi leader online
- ACC1 mặc định là leader.
- Có thể chọn account khác đang online, kể cả chỉ có một account online và chưa có party.
- Nếu đang có party: về an toàn, rời party cũ, leader mới mời lại đội.
- Xác nhận party rồi mới hoàn tất đổi vai trò.
- Nếu trước đó đang farm, có cơ chế tiếp tục mục tiêu farm.
- Không cho đổi giữa một số luồng daily/phó bản đang hoạt động.
- Lựa chọn leader hiện được giữ trong phiên chạy, chưa lưu bền vững như thiết lập combat.
10. Reconnect và phục hồi team
Có reconnect tự động khi mất kết nối:
- Thử nhanh lúc đầu, sau đó tăng khoảng nghỉ.
- Mỗi account có vòng kết nối riêng.
- Có cơ chế phục hồi mục tiêu farm và trạng thái daily đang làm.
Luồng dự kiến đang được xử lý trong code:
- Leader dis: member về an toàn/chờ leader rồi lập lại đội.
- Member dis/chết: reconnect, đồng bộ với leader và tham gia lại.
- Toàn đội dis: reconnect rồi điều phối lại.
- Nhận tín hiệu bảo trì được nhận diện: dừng toàn bộ account, không tiếp tục reconnect.
Không phải mọi lỗi socket đều được coi là bảo trì. Các trường hợp phục hồi team này vẫn cần test thực tế thêm.
11. EXP, nhật ký và hiệu suất
Từng account có:
- HP/SP nhân vật và pet, hiện tên thật.
- EXP hiện tại/EXP cần lên cấp khi xác định được.
- Vàng, tiền đồng.
- Tiến độ boss và phó bản.
- Log EXP nhân vật/pet, nhặt vật phẩm, sử dụng vật phẩm.
- Số trận hoàn tất.
- Thời gian trận gần nhất.
- Số trận/giờ, EXP nhân vật/giờ, EXP pet/giờ.
Cách tính hiệu suất:
Tổng nhận được trong phiên ÷ thời gian phiên × 1 giờ
Thời gian phiên bao gồm khoảng nghỉ giữa trận, nên không phải tốc độ chỉ tính lúc đang combat. Phiên quá ngắn thì số trận/giờ dễ dao động.
Giới hạn EXP hiện tại:
- Mốc quy đổi đã đối chiếu chỉ có một số cấp: nhân vật 155, pet 174 và 184.
- Các cấp khác chưa có đủ mốc thì không thể coi EXP lên cấp là đã chính xác.
- EXP nhận được của nhân vật vẫn chưa được xác nhận đầy đủ như pet. Không có packet tăng EXP được nhận diện thì log/rate nhân vật có thể bằng 0.
12. Update, debug và bộ nhớ
Check Update
Kiểm tra GitHub Release → so phiên bản → tải APK mới → Android hỏi cài cập nhật
- Push source lên Git không tự tạo APK.
- Cần có Release chứa APK.
- Cơ chế hiện không có xác thực cho Release private.
- Android vẫn yêu cầu quyền cài từ nguồn ngoài và xác nhận cài.
- APK phải đúng package/chữ ký để cập nhật đè bản cũ.
Packet/debug
- Có xem packet server dưới dạng JSON.
- Có capture và xuất file để điều tra.
- File capture có giới hạn và xoay vòng, không giữ toàn bộ lịch sử vô hạn.
- Một số dữ liệu đăng nhập đã che, nhưng file vẫn có thể chứa thông tin account/game.
Bộ nhớ/log
- Nhật ký UI giới hạn số dòng.
- Log và capture có xoay file theo dung lượng.
- Bản đồ có cache dữ liệu địa hình và tái sử dụng view.
- Chưa thể khẳng định đã loại hết memory leak nếu chưa đo chạy dài hạn.
