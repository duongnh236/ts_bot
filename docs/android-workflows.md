# Android workflows (v119)

Luồng chuẩn theo yêu cầu ngày 18/09/2026. Những luồng này sử dụng battle/heal config theo account đã lưu; không ghi đè rule skill hoặc ngưỡng HP/SP khi đổi luồng. Pet đã chọn trong UI được ưu tiên hơn pet role mặc định; chưa chọn thì giữ cơ chế mặc định.

## Train theo map

1. Chốt các account đang online khi bấm Bắt đầu farm; leader phải online.
2. Leader chọn thành gần map train bằng dữ liệu smart route, công bố cùng một kế hoạch cho các member.
3. Mỗi account đánh xong trận, dừng di chuyển Dị giới nếu có, thoát Dị giới/rời party cũ, phù thẳng về thành đã chọn. Không teleport trung gian random hoặc bán/cất đồ trong bước này.
4. Chờ đủ account được server xác nhận đã tới thành. Leader công bố phân khu live tại thành; member chuyển tới đúng phân khu đó.
5. Member mở nhận lời mời trước khi chờ leader. Leader mời và kiểm tra đúng entity của từng member trong roster server, cùng map và phân khu. Đủ đội thì chọn quân sư.
6. Chỉ leader chạy smart route/Ground path ra map train rồi tới tọa độ chọn. Member theo party; combat được bật trên đường và dùng các rule đã cấu hình.
7. Thành công: `ui_train_phase=farming`; điều phối nền mới được tiếp quản phục hồi team. Thất bại: giữ online, log lý do; không chạy tiếp một mình, không tự relogin chỉ vì không gặp quái.

Không còn fast-path bỏ về thành khi đã ở map train. Command generation mới hủy đường đi của generation cũ; retry giữ nguyên loại Train và tọa độ.

Entry: `agent_bridge.start_farm_mode_json('train', ...)` → `auto_battle_team_json` → `party_train_map` → `_do_manual_cmd('train')` → `_do_manual_route` → leader `navigate_to`.

## Dị giới + Train theo map

Chốt các account online, lưu riêng mục tiêu farm, chuyển runtime vào Dị giới. Chờ thời gian server xác nhận của tất cả các account tham gia; trạng thái chưa biết/reconnect không được coi là đã hết giờ. Account xong trước ra điểm chờ.

Khi toàn bộ xong: `_android_dg_train_handoff` đợi tất cả worker thoát Dị giới và trở lại vòng nhận command trên cùng kết nối. Chỉ phát một lệnh `party_train_map`; từ đây chạy đúng flow Train ở trên, không có route farm riêng của Dị giới.

## Daily quest

Chỉ chạy khi bấm nút chạy daily đã chọn. Thu hồi mục tiêu farm/Dị giới và dừng điều phối farm. Mỗi account đánh xong trận, rời party farm, phù về Trác Quận bằng `_daily_return_to_city`.

- Boss quân đoàn, phụ bản đơn, boss thế giới: chạy độc lập theo lượt/cooldown/điều kiện server; không chờ account khác giữa các task.
- Phó bản tổ đội: chạy trước các daily solo nếu đã chọn. Tập trung tại Trác Quận, giữ phân khu mà server cho vào; không ép cùng phân khu và không lập party farm. Leader tạo phòng theo cấp đã chọn, mời account đủ điều kiện; member nhận lời mời phòng và chuẩn bị; chỉ start theo cơ chế ready của phó bản. Không lấy ready/roster của party farm thay cho room-ready.
- Các task đội đồng bộ với nhau; các task solo không có team barrier.
- Chạy xong hoặc bấm Dừng: đứng yên và giữ online. Không tự quay lại farm/Dị giới; muốn farm thì bấm Bắt đầu farm lại.

Lỗi teleport hoặc room không hợp lệ được ghi log và giữ online. Điều kiện level/lượt vẫn tuân theo server; không giả lập đủ điều kiện hoặc ACK thành công.

## Kiểm chứng

`tests/test_android_safety.py` chạy AST thực của handler Train với 5 worker song song và mock server: tất cả về thành, member theo phân khu leader, mở nhận lời mời, đủ roster rồi mới gọi route và Ground navigation của leader. Các test khác kiểm tra handoff Dị giới một lần, daily solo không phụ thuộc team, pet đã chọn/mặc định, command priority và generation cancellation.

Đây là kiểm thử logic, không thay thế lượt test online. Xuất log JSON khi đang kẹt, trước Logout All; các trường `ui_train_phase`, route plan, city-arrived, party-invite-ready và roster count giúp xác định bước chưa được server xác nhận.
