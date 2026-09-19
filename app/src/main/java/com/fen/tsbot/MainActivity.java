package com.fen.tsbot;

import android.Manifest;
import android.app.*;
import android.content.*;
import android.content.pm.PackageManager;
import android.content.res.ColorStateList;
import android.graphics.Color;
import android.graphics.Typeface;
import android.os.*;
import android.text.InputType;
import android.text.SpannableString;
import android.text.Spanned;
import android.text.style.ForegroundColorSpan;
import android.view.*;
import android.widget.*;
import com.chaquo.python.PyObject;
import com.chaquo.python.Python;
import com.chaquo.python.android.AndroidPlatform;
import org.json.*;
import java.util.*;
import java.util.concurrent.*;

public class MainActivity extends Activity {
    private static final int BG = Color.rgb(8,17,31), CARD = Color.rgb(17,29,48), GOLD = Color.rgb(213,168,78), BLUE = Color.rgb(74,163,255);
    private final EditText[] users = new EditText[5], passes = new EditText[5];
    private final Spinner[] charSkills = new Spinner[5], petSkills = new Spinner[5];
    private final TextView[] petInfo = new TextView[5];
    private final List<List<Integer>> charSkillIds = new ArrayList<>(), petSkillIds = new ArrayList<>();
    private final CheckBox[] enabled = new CheckBox[5];
    private final List<JSONObject> servers = new ArrayList<>(), maps = new ArrayList<>();
    private Spinner serverSpinner, mapSpinner, modeSpinner, digioiLevelSpinner, farmPointSpinner; private EditText farmX, farmY; private TextView status, selectionInfo,dailyStatus,updateStatus; private Button dailyStop,updateButton,startFarmButton;
    private SharedPreferences trainPrefs; private boolean restoringTrainSelection=false;
    private Button switchLeaderButton,loginAllButton,logoutAllButton;
    private boolean loginAllPending=false,loginAllAcknowledged=false,logoutAllPending=false;
    private View configView; private TeamMapView teamMapView; private AccountManagerView accountManagerView; private FrameLayout pageHost;
    private final Button[] accountNavButtons=new Button[5]; private Button controlNavButton; private int currentPage=0,selectedAccount=0; private JSONArray bottomAccounts=new JSONArray();
    private final Handler handler = new Handler(Looper.getMainLooper());
    // Queue co gioi han + bo qua mot nhip poll khi nhip truoc chua xu ly xong. Neu Python/server
    // cham, Executors.newSingleThreadExecutor cu se tich vo han 4 task moi 2.5 giay va an RAM.
    private final ThreadPoolExecutor io = new ThreadPoolExecutor(1,1,0L,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(32),new ThreadPoolExecutor.AbortPolicy());
    private final Runnable poll = new Runnable() { public void run() { if(!io.isShutdown()&&io.getQueue().isEmpty()){refreshStatus();refreshAccountsDashboard();refreshDailyStatus();}handler.postDelayed(this,2500); } };
    private volatile boolean mapRefreshPending=false;
    private final Runnable mapPoll=new Runnable(){public void run(){if(!io.isShutdown()&&io.getQueue().isEmpty()&&!mapRefreshPending&&(currentPage==1||(currentPage==2&&accountManagerView!=null&&accountManagerView.isMapSection())))refreshMapSnapshot();handler.postDelayed(this,1000);}};
    private final BroadcastReceiver resultReceiver = new BroadcastReceiver() {
        public void onReceive(Context c, Intent i) { String raw=i.getStringExtra("json");int resultSlot=i.getIntExtra("slot",-1);String resultUser=i.getStringExtra("user");status.setText(raw);try{JSONObject r=new JSONObject(raw);String msg=r.optString("message",raw);status.setText(msg);boolean error=!r.optBoolean("ok");if(loginAllPending&&resultSlot<0){loginAllAcknowledged=true;if(error){loginAllPending=false;accountManagerView.clearLoginPending("");}refreshAccountsDashboard();updateTeamActionButtons();}else if(resultSlot>=0){if(error&&accountManagerView!=null)accountManagerView.clearLoginPending(resultSlot);if(accountManagerView!=null)accountManagerView.setActionMessage(resultSlot,msg,error);}else if(accountManagerView!=null&&resultUser!=null&&!resultUser.isEmpty())accountManagerView.setActionMessageForUser(resultUser,msg,error);Toast.makeText(MainActivity.this,msg,Toast.LENGTH_LONG).show();}catch(Exception e){if(accountManagerView!=null&&resultSlot>=0)accountManagerView.setActionMessage(resultSlot,raw,true);} }
    };

    @Override public void onCreate(Bundle b) {
        super.onCreate(b);
        AssetInstaller.ensure(this);
        trainPrefs=getSharedPreferences("tsbot_train_selection",MODE_PRIVATE);
        for(int i=0;i<5;i++){charSkillIds.add(new ArrayList<>(Collections.singletonList(0)));petSkillIds.add(new ArrayList<>(Collections.singletonList(0)));}
        if (!Python.isStarted()) Python.start(new AndroidPlatform(this));
        if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED)
            requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, 5);
        setContentView(buildTabbedUi());
        registerReceiver(resultReceiver, new IntentFilter(BotService.RESULT), RECEIVER_NOT_EXPORTED);
        io.execute(this::loadCatalog);
    }

    private View buildTabbedUi(){LinearLayout app=column();app.setBackgroundColor(BG);pageHost=new FrameLayout(this);configView=buildUi();teamMapView=new TeamMapView(this);accountManagerView=new AccountManagerView(this);accountManagerView.setAccountActionListener(new AccountManagerView.AccountActionListener(){public void onLogin(int slot,String user,String pass){startOne(slot,user,pass);}public void onAutoBattle(int slot,String user,String pass){autoBattleOne(slot,user,pass);}public void onLogout(int slot,String user){stopOne(slot,user);}public void onTeleport(int slot,String user,int cityId){teleportOne(slot,user,cityId);}public void onCombatSettings(int slot,String user,int petId,int charSkill,int petSkill,int hpPercent,int spPercent,boolean usePhucThan,boolean useDaiPhucThan,int charMobMin,int petMobMin,int petHpPercent,int petSpPercent,boolean useDgHoPhu,boolean autoBuyBaoHop){applyCombatSettings(slot,user,petId,charSkill,petSkill,hpPercent,spPercent,usePhucThan,useDaiPhucThan,charMobMin,petMobMin,petHpPercent,petSpPercent,useDgHoPhu,autoBuyBaoHop);}public void onAccountAction(String user,String action,JSONObject payload){runAccountAction(user,action,payload);}public void onChannelPolicy(boolean autoMode,int channel){applyChannelPolicy(autoMode,channel);}public void onMapTap(int x,int y){moveTeamFromMap(x,y);}});teamMapView.setOnMapTapListener(this::moveTeamFromMap);pageHost.addView(configView,new FrameLayout.LayoutParams(-1,-1));pageHost.addView(teamMapView,new FrameLayout.LayoutParams(-1,-1));pageHost.addView(accountManagerView,new FrameLayout.LayoutParams(-1,-1));teamMapView.setVisibility(View.GONE);accountManagerView.setVisibility(View.GONE);app.addView(pageHost,new LinearLayout.LayoutParams(-1,0,1));app.addView(buildBottomNav(),new LinearLayout.LayoutParams(-1,-2));return app;}
    private View buildBottomNav(){HorizontalScrollView scroll=new HorizontalScrollView(this);scroll.setFillViewport(true);scroll.setClipToPadding(false);scroll.setBackgroundColor(Color.rgb(5,12,22));LinearLayout nav=row();nav.setPadding(dp(6),dp(7),dp(6),dp(20));for(int i=0;i<5;i++){final int slot=i;accountNavButtons[i]=button("○ ACC "+(i+1),Color.rgb(25,35,49),Color.WHITE);accountNavButtons[i].setOnClickListener(v->showAccountPage(slot));LinearLayout.LayoutParams lp=new LinearLayout.LayoutParams(dp(96),dp(62));lp.setMargins(dp(2),0,dp(2),0);nav.addView(accountNavButtons[i],lp);}controlNavButton=button("⚙ ĐIỀU KHIỂN",GOLD,Color.rgb(20,20,20));controlNavButton.setOnClickListener(v->showPage(0));LinearLayout.LayoutParams cp=new LinearLayout.LayoutParams(dp(142),dp(62));cp.setMargins(dp(3),0,dp(3),0);nav.addView(controlNavButton,cp);scroll.addView(nav);updateBottomNav();return scroll;}
    private void showPage(int page){currentPage=page;configView.setVisibility(page==0?View.VISIBLE:View.GONE);teamMapView.setVisibility(page==1?View.VISIBLE:View.GONE);accountManagerView.setVisibility(page==2?View.VISIBLE:View.GONE);updateBottomNav();updateTeamActionButtons();if(page==1)refreshMapSnapshot();if(page==2)refreshAccountsDashboard();}
    private void showAccountPage(int slot){selectedAccount=slot;accountManagerView.selectAccount(slot);showPage(2);}
    private void updateBottomNav(){for(int i=0;i<5;i++){Button b=accountNavButtons[i];if(b==null)continue;JSONObject a=i<bottomAccounts.length()?bottomAccounts.optJSONObject(i):null;boolean online=a!=null&&a.optBoolean("online");boolean active=currentPage==2&&selectedAccount==i;String label="●  ACC "+(i+1);SpannableString text=new SpannableString(label);text.setSpan(new ForegroundColorSpan(online?Color.rgb(58,225,128):Color.rgb(115,125,140)),0,1,Spanned.SPAN_EXCLUSIVE_EXCLUSIVE);b.setText(text);b.setTextColor(Color.WHITE);b.setTypeface(null,active?Typeface.BOLD:Typeface.NORMAL);b.setBackgroundTintList(ColorStateList.valueOf(active?Color.rgb(38,105,178):(online?Color.rgb(22,72,57):Color.rgb(28,38,53))));}if(controlNavButton!=null){boolean active=currentPage!=2;controlNavButton.setBackgroundTintList(ColorStateList.valueOf(active?GOLD:Color.rgb(62,52,34)));controlNavButton.setTextColor(active?Color.rgb(20,20,20):Color.WHITE);}}

    private View buildUi() {
        ScrollView scroll = new ScrollView(this); scroll.setBackgroundColor(BG);
        LinearLayout root = column(); root.setPadding(dp(16), dp(18), dp(16), dp(28)); scroll.addView(root);
        TextView title = text("TS BOT", 29, GOLD); title.setTypeface(Typeface.DEFAULT_BOLD); root.addView(title);
        TextView sub = text("ACCOUNT MANAGER  •  ANDROID DIRECT", 12, Color.LTGRAY); root.addView(sub);
        updateButton=button("⬇  KIỂM TRA CẬP NHẬT",Color.rgb(30,112,82),Color.WHITE);
        updateButton.setOnClickListener(v->checkForUpdate());root.addView(updateButton,matchWrap());
        updateStatus=text("Phiên bản đang dùng: "+UpdateManager.currentVersionName(MainActivity.this),12,Color.rgb(155,185,205));
        updateStatus.setPadding(dp(10),dp(7),dp(10),dp(9));root.addView(updateStatus,matchWrap());
        Button daily=button("📅  DAILY QUEST",Color.rgb(126,82,190),Color.WHITE);daily.setOnClickListener(v->showDailyQuestDialog());root.addView(daily,matchWrap());
        dailyStatus=text("📅 Daily: chưa chạy",14,Color.rgb(185,205,230));dailyStatus.setPadding(dp(12),dp(10),dp(12),dp(10));dailyStatus.setBackgroundColor(CARD);root.addView(dailyStatus,matchWrap());
        dailyStop=button("■  DỪNG DAILY",Color.rgb(170,58,68),Color.WHITE);dailyStop.setEnabled(false);dailyStop.setVisibility(View.GONE);dailyStop.setOnClickListener(v->stopDaily());root.addView(dailyStop,matchWrap());
        root.addView(space(14));
        TextView note = text("Kết nối trực tiếp TS Online. Quản lý từng account trong tab ACC; chỉ lưu mật khẩu khi bật Lưu tài khoản.", 13, Color.rgb(185,198,215));
        note.setPadding(dp(12),dp(10),dp(12),dp(10)); note.setBackgroundColor(CARD); root.addView(note, matchWrap());
        root.addView(section("CẤU HÌNH PARTY"));
        switchLeaderButton=button("👑 ĐỔI LEADER ONLINE",Color.rgb(145,100,30),Color.WHITE);switchLeaderButton.setOnClickListener(v->showLeaderSwitchDialog());root.addView(switchLeaderButton,matchWrap());
        serverSpinner = spinner(); modeSpinner = spinner(); digioiLevelSpinner=spinner(); mapSpinner = spinner(); farmPointSpinner=spinner();
        modeSpinner.setAdapter(adapter(Arrays.asList("Train theo map", "Đứng yên", "Dị giới + farm")));
        digioiLevelSpinner.setAdapter(adapter(Arrays.asList("Cấp 10","Cấp 25","Cấp 40","Cấp 55","Cấp 70","Cấp 85","Cấp 100","Cấp 110","Cấp 120","Cấp 130","Cấp 140","Cấp 150","Cấp 160","Cấp 170","Cấp 180")));
        root.addView(label("Server")); root.addView(serverSpinner, matchWrap());
        root.addView(label("Chế độ")); root.addView(modeSpinner, matchWrap());
        root.addView(label("Cấp độ Dị Giới")); root.addView(digioiLevelSpinner, matchWrap());
        root.addView(label("Bãi train")); root.addView(mapSpinner, matchWrap());
        root.addView(label("Điểm farm")); root.addView(farmPointSpinner,matchWrap());
        LinearLayout xy=row(); farmX=input("Tọa độ X",false);farmY=input("Tọa độ Y",false);farmX.setInputType(InputType.TYPE_CLASS_NUMBER);farmY.setInputType(InputType.TYPE_CLASS_NUMBER);xy.addView(farmX,weight());xy.addView(farmY,weight());root.addView(xy,matchWrap());
        selectionInfo=text("Đang nạp lựa chọn…",12,Color.rgb(160,190,225));selectionInfo.setPadding(0,dp(8),0,0);root.addView(selectionInfo,matchWrap());
        AdapterView.OnItemSelectedListener selected=new AdapterView.OnItemSelectedListener(){public void onItemSelected(AdapterView<?>p,View v,int pos,long id){if(p==mapSpinner)updateFarmPoints();updateSelectionInfo();if(!restoringTrainSelection)saveTrainSelection();}public void onNothingSelected(AdapterView<?>p){}};serverSpinner.setOnItemSelectedListener(selected);modeSpinner.setOnItemSelectedListener(selected);digioiLevelSpinner.setOnItemSelectedListener(selected);mapSpinner.setOnItemSelectedListener(selected);farmPointSpinner.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener(){public void onItemSelected(AdapterView<?>p,View v,int pos,long id){applyFarmPoint(pos);updateSelectionInfo();if(!restoringTrainSelection)saveTrainSelection();}public void onNothingSelected(AdapterView<?>p){}});View.OnFocusChangeListener saveCoordinate=(v,hasFocus)->{if(!hasFocus&&!restoringTrainSelection)saveTrainSelection();};farmX.setOnFocusChangeListener(saveCoordinate);farmY.setOnFocusChangeListener(saveCoordinate);
        startFarmButton=button("▶  BẮT ĐẦU FARM",Color.rgb(32,150,92),Color.WHITE);startFarmButton.setTextSize(17);startFarmButton.setTypeface(Typeface.DEFAULT_BOLD);startFarmButton.setOnClickListener(v->startFarmTeam());LinearLayout.LayoutParams farmLp=new LinearLayout.LayoutParams(-1,dp(64));farmLp.setMargins(0,dp(12),0,dp(6));root.addView(startFarmButton,farmLp);
        TextView farmFlow=text("Train theo map: tất cả account online phù về thành gần bãi → theo phân khu leader → lập đủ party → leader kéo ra tọa độ farm. Dị giới + farm: hết giờ Dị giới rồi chạy cùng luồng này.",13,Color.rgb(175,215,195));farmFlow.setPadding(dp(12),dp(10),dp(12),dp(10));farmFlow.setBackgroundColor(Color.rgb(13,45,37));root.addView(farmFlow,matchWrap());
        for (int i=0; i<5; i++) accountCard(i); // tao model control; form login nam trong tung bottom tab
        LinearLayout allAccountActions=row();Button loginAll=loginAllButton=button("🔑  LOGIN ALL",Color.rgb(30,125,200),Color.WHITE);Button logoutAll=logoutAllButton=button("⏻  LOGOUT ALL",Color.rgb(175,62,72),Color.WHITE);loginAll.setOnClickListener(v->loginAll());logoutAll.setVisibility(View.GONE);logoutAll.setOnClickListener(v->logoutAllSafe());allAccountActions.addView(loginAll,weight());allAccountActions.addView(logoutAll,weight());root.addView(allAccountActions,matchWrap());
        TextView accountHint=text("LOGIN/OUT riêng nằm trong từng tab ACC. LOGIN ALL và LOGOUT ALL thao tác một lần cho toàn bộ account đã cấu hình.",13,Color.rgb(170,195,220));accountHint.setPadding(dp(10),dp(12),dp(10),dp(12));accountHint.setBackgroundColor(CARD);root.addView(accountHint,matchWrap());
        root.addView(section("NHẬT KÝ TEAM"));
        status = text("Đang nạp dữ liệu server và bản đồ…", 13, Color.rgb(162,220,255));
        status.setTypeface(Typeface.MONOSPACE);status.setSingleLine(false);status.setHorizontallyScrolling(false);status.setMaxLines(Integer.MAX_VALUE);status.setPadding(dp(12),dp(12),dp(12),dp(12));status.setBackgroundColor(Color.rgb(5,12,22));
        if(Build.VERSION.SDK_INT>=23)status.setBreakStrategy(android.text.Layout.BREAK_STRATEGY_SIMPLE);
        status.setOnClickListener(v->showFullTeamLog());
        ScrollView teamLogScroll=new ScrollView(this);teamLogScroll.setFillViewport(true);teamLogScroll.setNestedScrollingEnabled(true);teamLogScroll.setVerticalScrollBarEnabled(true);teamLogScroll.addView(status,new ScrollView.LayoutParams(-1,-2));root.addView(teamLogScroll,new LinearLayout.LayoutParams(-1,dp(420)));
        TextView logHint=text("Chạm vào khung log để xem toàn màn hình • vuốt ngay trong khung để xem phần cuối",12,Color.rgb(145,170,195));logHint.setPadding(dp(8),dp(6),dp(8),dp(8));root.addView(logHint,matchWrap());
        Button exportTeam=button("⬇  XUẤT LOG TEAM JSON",Color.rgb(38,100,160),Color.WHITE);exportTeam.setOnClickListener(v->{Intent pick=new Intent(Intent.ACTION_CREATE_DOCUMENT);pick.addCategory(Intent.CATEGORY_OPENABLE);pick.setType("application/json");pick.putExtra(Intent.EXTRA_TITLE,"tsbot-team-debug-"+System.currentTimeMillis()+".json");startActivityForResult(pick,716);});root.addView(exportTeam,matchWrap());
        return scroll;
    }

    private void showFullTeamLog(){
        TextView content=text(status==null?"Chưa có log":status.getText().toString(),12,Color.WHITE);
        content.setTypeface(Typeface.MONOSPACE);content.setSingleLine(false);content.setHorizontallyScrolling(false);content.setTextIsSelectable(true);content.setPadding(dp(14),dp(14),dp(14),dp(20));
        if(Build.VERSION.SDK_INT>=23)content.setBreakStrategy(android.text.Layout.BREAK_STRATEGY_SIMPLE);
        ScrollView scroll=new ScrollView(this);scroll.setFillViewport(true);scroll.setBackgroundColor(Color.rgb(5,12,22));scroll.addView(content,new ScrollView.LayoutParams(-1,-2));
        AlertDialog dialog=new AlertDialog.Builder(this).setTitle("NHẬT KÝ TEAM • TOÀN BỘ DỮ LIỆU").setView(scroll).setPositiveButton("ĐÓNG",null).create();
        dialog.setOnShowListener(x->{Window window=dialog.getWindow();if(window!=null)window.setLayout(ViewGroup.LayoutParams.MATCH_PARENT,ViewGroup.LayoutParams.MATCH_PARENT);});dialog.show();
    }

    private void checkForUpdate(){
        if(updateButton==null)return;
        updateButton.setEnabled(false);updateButton.setText("⌛  ĐANG KIỂM TRA…");
        updateStatus.setText("Đang kiểm tra GitHub Releases…");
        UpdateManager.checkLatest(this,new UpdateManager.Listener(){
            public void onStatus(String message){runOnUiThread(()->updateStatus.setText(message));}
            public void onUpToDate(String version){runOnUiThread(()->{updateButton.setEnabled(true);updateButton.setText("✓  ĐÃ LÀ BẢN MỚI NHẤT");updateStatus.setText("Đang dùng "+UpdateManager.currentVersionName(MainActivity.this)+" • GitHub: "+version);Toast.makeText(MainActivity.this,"App đã là bản mới nhất",Toast.LENGTH_LONG).show();handler.postDelayed(()->updateButton.setText("⬇  KIỂM TRA CẬP NHẬT"),3000);});}
            public void onUpdate(UpdateManager.Release release){runOnUiThread(()->new AlertDialog.Builder(MainActivity.this).setTitle("Có bản cập nhật "+release.versionName).setMessage("Bản đang dùng: "+UpdateManager.currentVersionName(MainActivity.this)+"\nBản mới: "+release.versionName+"\n\nApp sẽ tải APK rồi mở màn hình xác nhận cài đặt của Android.").setNegativeButton("Để sau",(d,w)->resetUpdateButton()).setPositiveButton("TẢI VÀ CÀI",(d,w)->downloadUpdate(release)).setOnCancelListener(d->resetUpdateButton()).show());}
            public void onError(String message){runOnUiThread(()->{resetUpdateButton();updateStatus.setText("Không kiểm tra được cập nhật: "+message);Toast.makeText(MainActivity.this,"Lỗi cập nhật: "+message,Toast.LENGTH_LONG).show();});}
        });
    }
private void showLeaderSwitchDialog(){List<String> names=new ArrayList<>(),ids=new ArrayList<>();for(int i=0;i<bottomAccounts.length();i++){JSONObject a=bottomAccounts.optJSONObject(i);if(a!=null&&a.optBoolean("online")){ids.add(a.optString("user"));names.add(a.optString("name",a.optString("user"))+(a.optBoolean("leader")?" • LEADER HIỆN TẠI":""));}}if(ids.isEmpty()){Toast.makeText(this,"Cần ít nhất 1 account online",Toast.LENGTH_LONG).show();return;}new AlertDialog.Builder(this).setTitle("Chọn leader mới").setItems(names.toArray(new String[0]),(d,index)->new AlertDialog.Builder(this).setTitle("Đổi leader sang "+names.get(index)).setMessage("Nếu chưa có party: chọn trực tiếp, không cần ACC1 online. Nếu đã có party: chờ hết trận → leader cũ kéo team về SAFE → giải tán party → leader mới mời lại team. Không logout account. Nếu đang farm sẽ tiếp tục bãi đã chọn sau khi đổi xong.").setNegativeButton("HỦY",null).setPositiveButton("ĐỔI LEADER",(confirm,w)->switchOnlineLeader(ids.get(index))).show()).setNegativeButton("ĐÓNG",null).show();}
    private void switchOnlineLeader(String user){switchLeaderButton.setEnabled(false);switchLeaderButton.setText("⌛ ĐANG ĐỔI LEADER…");startFarmButton.setEnabled(false);status.setText("Đang chờ hết trận, về SAFE và lập lại party với leader mới…");io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("switch_leader_json",user).toString());runOnUiThread(()->{if(isFinishing()||isDestroyed())return;switchLeaderButton.setEnabled(true);switchLeaderButton.setText("👑 ĐỔI LEADER ONLINE");startFarmButton.setEnabled(true);status.setText(r.optString("message"));Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();refreshAccountsDashboard();});}catch(Exception e){runOnUiThread(()->{if(isFinishing()||isDestroyed())return;switchLeaderButton.setEnabled(true);switchLeaderButton.setText("👑 ĐỔI LEADER ONLINE");startFarmButton.setEnabled(true);status.setText("Lỗi đổi leader: "+e.getMessage());});}});}
    private void showServerPackets(){io.execute(()->{try{String raw=Python.getInstance().getModule("agent_bridge").callAttr("server_packets_json").toString();runOnUiThread(()->{if(isFinishing()||isDestroyed())return;TextView content=text(raw,11,Color.WHITE);content.setTypeface(Typeface.MONOSPACE);content.setTextIsSelectable(true);content.setPadding(dp(12),dp(12),dp(12),dp(12));ScrollView scroll=new ScrollView(this);scroll.setBackgroundColor(BG);scroll.addView(content);new AlertDialog.Builder(this).setTitle("100 packet server gần nhất • JSON").setView(scroll).setPositiveButton("ĐÓNG",null).show();});}catch(Exception e){runOnUiThread(()->Toast.makeText(this,"Không đọc được packet JSON: "+e.getMessage(),Toast.LENGTH_LONG).show());}});}

    private void downloadUpdate(UpdateManager.Release release){
        updateButton.setText("⬇  ĐANG TẢI APK…");updateButton.setEnabled(false);
        UpdateManager.downloadAndInstall(this,release,new UpdateManager.Listener(){
            public void onStatus(String message){runOnUiThread(()->updateStatus.setText(message));}
            public void onUpToDate(String version){}
            public void onUpdate(UpdateManager.Release ignored){}
            public void onError(String message){runOnUiThread(()->{resetUpdateButton();updateStatus.setText("Cập nhật thất bại: "+message);Toast.makeText(MainActivity.this,message,Toast.LENGTH_LONG).show();});}
        });
    }

    private void resetUpdateButton(){if(updateButton!=null){updateButton.setEnabled(true);updateButton.setText("⬇  KIỂM TRA CẬP NHẬT");}}

    private void showDailyQuestDialog(){
        String[] keys={"legion_boss","world_boss","solo_dungeon","team_dungeon"};
        String[] names={"⚔  Đánh boss quân đoàn","🌍  Đánh boss thế giới","🏯  Đánh phụ bản đơn","👥  Đánh phụ bản tổ đội"};
        LinearLayout box=column();box.setPadding(dp(18),dp(12),dp(18),dp(16));box.setBackgroundColor(Color.rgb(12,24,41));
        TextView title=text("📅  DAILY QUEST",22,GOLD);title.setTypeface(Typeface.DEFAULT_BOLD);box.addView(title);
        TextView note=text("Tick các nhiệm vụ cần làm. Chỉ bắt đầu khi bấm nút CHẠY CÁC DAILY ĐÃ TICK bên dưới.",13,Color.rgb(175,195,220));note.setPadding(0,dp(6),0,dp(12));box.addView(note);
        CheckBox[] checks=new CheckBox[keys.length];
        CheckBox[] levelChecks=new CheckBox[4];int[] levels={20,50,80,110};
        ScrollView dailyScroll=new ScrollView(this);
        dailyScroll.setFillViewport(true);
        dailyScroll.setClipToPadding(false);
        dailyScroll.addView(box,new ScrollView.LayoutParams(-1,-2));
        AlertDialog dialog=new AlertDialog.Builder(this).setView(dailyScroll).setNegativeButton("ĐÓNG",null).create();
        for(int i=0;i<keys.length;i++){
            LinearLayout row=row();row.setGravity(Gravity.CENTER_VERTICAL);row.setPadding(dp(8),dp(5),dp(4),dp(5));row.setBackgroundColor(i%2==0?Color.rgb(19,36,58):Color.rgb(15,30,49));
            checks[i]=new CheckBox(this);checks[i].setText(names[i]);checks[i].setTextColor(Color.WHITE);checks[i].setTextSize(15);checks[i].setChecked(false);row.addView(checks[i],new LinearLayout.LayoutParams(0,dp(58),1));
            LinearLayout.LayoutParams rp=matchWrap();rp.setMargins(0,dp(3),0,dp(3));box.addView(row,rp);
            if(i==3){String[] dungeonNames={"Thảo Phạt Thiên Sư","Ngày Tàn Hoạn Quan","Đại Chiến Lữ Bố","Hỏa Thiêu Bộc Dương"};LinearLayout levelBox=column();levelBox.setPadding(dp(28),0,dp(6),dp(8));levelBox.setBackgroundColor(Color.rgb(10,22,38));for(int j=0;j<levels.length;j++){levelChecks[j]=new CheckBox(this);levelChecks[j].setText(dungeonNames[j]+"  •  Cấp "+levels[j]+(levels[j]==110?"  •  mặc định tắt":""));levelChecks[j].setTextColor(levels[j]==110?Color.rgb(180,190,205):Color.rgb(205,220,238));levelChecks[j].setChecked(levels[j]!=110);levelBox.addView(levelChecks[j],new LinearLayout.LayoutParams(-1,dp(45)));}box.addView(levelBox,new LinearLayout.LayoutParams(-1,-2));}
        }
        Button run=button("▶  CHẠY CÁC DAILY ĐÃ TICK",Color.rgb(38,125,210),Color.WHITE);run.setOnClickListener(v->{List<String> selected=new ArrayList<>();for(int i=0;i<3;i++)if(checks[i].isChecked())selected.add(keys[i]);if(checks[3].isChecked())selected.addAll(selectedTeamLevels(levelChecks,levels));runDailyTasks(selected);});
        LinearLayout.LayoutParams runLp=new LinearLayout.LayoutParams(-1,dp(60));runLp.setMargins(0,dp(12),0,0);box.addView(run,runLp);
        dialog.setOnShowListener(x->{Window w=dialog.getWindow();if(w!=null){w.setBackgroundDrawableResource(android.R.color.transparent);w.setLayout(-1,(int)(getResources().getDisplayMetrics().heightPixels*0.80f));}});dialog.show();
    }
    private List<String> selectedTeamLevels(CheckBox[] checks,int[] levels){List<String> out=new ArrayList<>();for(int i=0;i<levels.length;i++)if(checks[i]!=null&&checks[i].isChecked())out.add("team_dungeon_"+levels[i]);return out;}
    private void runDailyTasks(List<String> tasks){if(tasks==null||tasks.isEmpty()){Toast.makeText(this,"Chưa tick daily quest nào",Toast.LENGTH_SHORT).show();return;}JSONArray payload=new JSONArray();for(String task:tasks)payload.put(task);status.setText("Đang gửi lệnh DAILY QUEST…");io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("run_daily_tasks_json",payload.toString()).toString());runOnUiThread(()->{status.setText(r.optString("message"));Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();});}catch(Exception e){runOnUiThread(()->{status.setText("Lỗi Daily Quest: "+e.getMessage());Toast.makeText(this,"Lỗi Daily Quest: "+e.getMessage(),Toast.LENGTH_LONG).show();});}});}
    private void refreshDailyStatus(){
        if(dailyStatus==null)return;
        io.execute(()->{try{
            JSONObject d=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("daily_status_json").toString());
            if(!d.optBoolean("ok"))return;
            boolean active=d.optBoolean("daily_active");String phase=d.optString("daily_phase","");
            String quest=d.isNull("daily_task_name")?"":d.optString("daily_task_name","");
            String summary;
            if("stopping".equals(phase))summary="📅 Daily: chờ hết trận để dừng";
            else if(active)summary=quest.isEmpty()?"📅 Daily: đang chuẩn bị":"📅 Đang đi: "+quest;
            else if("completed".equals(phase))summary="📅 Daily: đã hoàn tất";
            else if("cancelled".equals(phase))summary="📅 Daily: đã dừng";
            else if("failed".equals(phase))summary="📅 Daily: tạm dừng do lỗi";
            else summary="📅 Daily: chưa chạy";
            final String label=summary;
            runOnUiThread(()->{dailyStatus.setText(label);dailyStatus.setTextColor(active?Color.rgb(100,215,255):Color.rgb(185,205,230));dailyStop.setEnabled(active&& !"stopping".equals(phase));dailyStop.setVisibility(active?View.VISIBLE:View.GONE);});
        }catch(Exception ignored){}});
    }
    private void stopDaily(){io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("stop_daily_json").toString());runOnUiThread(()->{Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();refreshDailyStatus();});}catch(Exception e){runOnUiThread(()->Toast.makeText(this,"Lỗi dừng Daily: "+e.getMessage(),Toast.LENGTH_LONG).show());}});}

    private View accountCard(int i) {
        LinearLayout c = column(); c.setPadding(dp(12),dp(10),dp(12),dp(12)); c.setBackgroundColor(CARD);
        enabled[i] = new CheckBox(this); enabled[i].setText("SLOT " + (i+1) + (i==0 ? "  •  LEADER" : "  •  MEMBER")); enabled[i].setTextColor(i==0?GOLD:Color.WHITE); enabled[i].setChecked(i==0); c.addView(enabled[i]);
        users[i]=input("Username",false); passes[i]=input("Password",true); c.addView(users[i],matchWrap()); c.addView(passes[i],matchWrap());
        petInfo[i]=text("Chưa login — chưa có dữ liệu nhân vật/pet",12,Color.rgb(145,165,190));c.addView(petInfo[i],matchWrap());
        LinearLayout skills=row(); charSkills[i]=spinner();petSkills[i]=spinner();charSkills[i].setAdapter(adapter(Collections.singletonList("Skill nhân vật: Tự động")));petSkills[i].setAdapter(adapter(Collections.singletonList("Skill pet: Tự động")));skills.addView(charSkills[i],weight());skills.addView(petSkills[i],weight());c.addView(skills,matchWrap());
        Button load=button("↻ LẤY PET & SKILL SAU LOGIN",Color.rgb(38,75,119),Color.WHITE);load.setOnClickListener(v->loadSkills(i));c.addView(load,matchWrap());
        LinearLayout.LayoutParams lp=matchWrap(); lp.setMargins(0,0,0,dp(10)); c.setLayoutParams(lp); return c;
    }

    private void loadCatalog() {
        try {
            String raw=Python.getInstance().getModule("agent_bridge").callAttr("catalog_json").toString(); JSONObject data=new JSONObject(raw);
            JSONArray ss=data.getJSONArray("servers"), mm=data.getJSONArray("maps");
            List<String> sl=new ArrayList<>(), ml=new ArrayList<>();
            for(int i=0;i<ss.length();i++){ JSONObject x=ss.getJSONObject(i); servers.add(x); sl.add(x.getString("label")); }
            maps.add(new JSONObject().put("id",0).put("name","Không dịch chuyển")); ml.add("0 • Không dịch chuyển");
            for(int i=0;i<mm.length();i++){ JSONObject x=mm.getJSONObject(i); maps.add(x); ml.add(x.getInt("id")+" • "+x.getString("name")); }
            runOnUiThread(() -> { serverSpinner.setAdapter(adapter(sl)); mapSpinner.setAdapter(adapter(ml)); modeSpinner.setSelection(0);restoreTrainSelection();status.setText("Sẵn sàng. Đã khôi phục bãi train và tọa độ lần trước."); });
        } catch(Exception e) { runOnUiThread(() -> status.setText("Không nạp được catalog: "+e)); }
    }

    private void startBot(boolean loginOnly) {
        try {
            JSONObject p=buildPayload(loginOnly,false);
            Intent in=new Intent(this,BotService.class).setAction(BotService.START).putExtra("payload",p.toString()); startForegroundService(in); status.setText(loginOnly?"Đang đăng nhập; APK sẽ tự lấy pet và skill khi server trả dữ liệu…":"Đang khởi động farm…");
            if(loginOnly){handler.postDelayed(()->autoLoadSkills(),10000);handler.postDelayed(()->autoLoadSkills(),20000);}
        } catch(Exception e){ status.setText("Lỗi cấu hình: "+e.getMessage()); }
    }
    private void loginAll(){if(loginAllPending||accountManagerView.hasPendingLogin()||allConfiguredAccountsOnline())return;try{JSONObject p=buildPayload(true,true);if(p.getJSONArray("accounts").length()==0)throw new Exception("Chưa có account nào đã nhập username");loginAllPending=true;loginAllAcknowledged=false;for(int i=0;i<5;i++)if(!accountManagerView.configuredUser(i).isEmpty())accountManagerView.markLoginPending(i);updateTeamActionButtons();startForegroundService(new Intent(this,BotService.class).setAction(BotService.START).putExtra("payload",p.toString()));status.setText("Đang LOGIN ALL các account đã cấu hình…");handler.postDelayed(()->autoLoadSkills(),10000);handler.postDelayed(()->autoLoadSkills(),20000);}catch(Exception e){loginAllPending=false;accountManagerView.clearLoginPending("");updateTeamActionButtons();status.setText("Lỗi LOGIN ALL: "+e.getMessage());Toast.makeText(this,"Lỗi: "+e.getMessage(),Toast.LENGTH_LONG).show();}}
    private JSONObject buildPayload(boolean loginOnly,boolean includeConfigured)throws Exception{
        if(servers.isEmpty()||serverSpinner.getSelectedItemPosition()<0)throw new Exception("Danh sách server chưa nạp xong");
        if(!loginOnly&&(maps.isEmpty()||mapSpinner.getSelectedItemPosition()<0))throw new Exception("Danh sách map chưa nạp xong");
        // Form cua 5 tab la nguon tai khoan; khong doc model form an chua duoc dong bo.
        if(accountManagerView!=null){
            JSONArray credentials=accountManagerView.getConfiguredCredentials();
            for(int i=0;i<5;i++){users[i].setText("");passes[i].setText("");}
            for(int j=0;j<credentials.length();j++){JSONObject c=credentials.getJSONObject(j);int slot=c.getInt("slot");users[slot].setText(c.getString("u"));passes[slot].setText(c.getString("p"));}
        }
        JSONObject p=new JSONObject();JSONArray aa=new JSONArray();java.util.Set<String> seen=new java.util.HashSet<>();
        for(int i=0;i<5;i++){
            String user=users[i].getText().toString().trim(),pass=passes[i].getText().toString();
            if((!includeConfigured&&!enabled[i].isChecked())||user.isEmpty())continue;
            if(pass.isEmpty())throw new Exception("ACC "+(i+1)+" chưa nhập mật khẩu");
            if(!seen.add(user))throw new Exception("Trùng tài khoản tại ACC "+(i+1));
            aa.put(new JSONObject().put("slot",i).put("on",true).put("u",user).put("p",pass).put("char_skill",selectedId(charSkillIds.get(i),charSkills[i])).put("pet_skill",selectedId(petSkillIds.get(i),petSkills[i])));
        }
        String[] modes={"train","stand","digioi_train"};
        return p.put("accounts",aa).put("server",servers.get(serverSpinner.getSelectedItemPosition())).put("channel",0).put("mode",loginOnly?"stand":modes[modeSpinner.getSelectedItemPosition()]).put("di_gioi_level",digioiLevelSpinner.getSelectedItemPosition()+1).put("map_id",loginOnly?0:maps.get(mapSpinner.getSelectedItemPosition()).getInt("id")).put("mob_index",-1).put("farm_x",loginOnly?0:num(farmX,0)).put("farm_y",loginOnly?0:num(farmY,0));
    }
    private JSONObject buildSingleLoginPayload(int slot,String user,String pass)throws Exception{if(servers.isEmpty()||serverSpinner.getSelectedItemPosition()<0)throw new Exception("Danh sách server chưa nạp xong");JSONArray aa=new JSONArray();aa.put(new JSONObject().put("slot",slot).put("on",true).put("u",user).put("p",pass).put("char_skill",selectedId(charSkillIds.get(slot),charSkills[slot])).put("pet_skill",selectedId(petSkillIds.get(slot),petSkills[slot])));return new JSONObject().put("accounts",aa).put("server",servers.get(serverSpinner.getSelectedItemPosition())).put("channel",0).put("mode","stand").put("di_gioi_level",digioiLevelSpinner.getSelectedItemPosition()+1).put("map_id",0).put("mob_index",-1).put("farm_x",0).put("farm_y",0).put("only_user",user);}
    private void startOne(int slot,String user,String pass){try{if(user==null||user.trim().isEmpty())throw new Exception("Chưa nhập username");if(pass==null||pass.isEmpty())throw new Exception("Chưa nhập password");String target=user.trim();users[slot].setText(target);passes[slot].setText(pass);enabled[slot].setChecked(true);JSONObject p=buildSingleLoginPayload(slot,target,pass);accountManagerView.markLoginPending(slot);startForegroundService(new Intent(this,BotService.class).setAction(BotService.START).putExtra("payload",p.toString()).putExtra("slot",slot).putExtra("user",target));String msg="Đang đăng nhập riêng "+target+" qua "+servers.get(serverSpinner.getSelectedItemPosition()).optString("label");accountManagerView.setActionMessage(slot,msg,false);Toast.makeText(this,msg,Toast.LENGTH_SHORT).show();}catch(Exception e){accountManagerView.clearLoginPending(slot);String msg="Lỗi đăng nhập: "+e.getMessage();if(accountManagerView!=null)accountManagerView.setActionMessage(slot,msg,true);Toast.makeText(this,msg,Toast.LENGTH_LONG).show();}}
    private void autoBattleOne(int slot,String user,String pass){if(user==null||user.trim().isEmpty()){Toast.makeText(this,"Chưa nhập username",Toast.LENGTH_SHORT).show();return;}users[slot].setText(user.trim());passes[slot].setText(pass==null?"":pass);int mapId=0,x=num(farmX,0),y=num(farmY,0);try{if(!maps.isEmpty())mapId=maps.get(mapSpinner.getSelectedItemPosition()).getInt("id");}catch(Exception ignored){}final int mid=mapId;io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("auto_battle_team_json",mid,x,y).toString());runOnUiThread(()->{accountManagerView.setActionMessage(slot,r.optString("message"),!r.optBoolean("ok"));Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();});}catch(Exception e){runOnUiThread(()->Toast.makeText(this,"Lỗi: "+e.getMessage(),Toast.LENGTH_LONG).show());}});}
    private void startFarmTeam(){int mapId=0,x=num(farmX,0),y=num(farmY,0);try{if(!maps.isEmpty())mapId=maps.get(mapSpinner.getSelectedItemPosition()).getInt("id");}catch(Exception ignored){}if(mapId<=0||x<=0||y<=0){Toast.makeText(this,"Hãy chọn bãi train và tọa độ farm trước",Toast.LENGTH_LONG).show();return;}final int mid=mapId,diGioiLevel=digioiLevelSpinner.getSelectedItemPosition()+1;final String mode=new String[]{"train","stand","digioi_train"}[modeSpinner.getSelectedItemPosition()];startFarmButton.setEnabled(false);startFarmButton.setText("⌛  ĐANG TẬP TRUNG TEAM…");status.setText("Đang chạy chế độ đã chọn…");io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("start_farm_mode_json",mode,mid,x,y,diGioiLevel).toString());runOnUiThread(()->{startFarmButton.setEnabled(true);startFarmButton.setText("▶  BẮT ĐẦU FARM");status.setText(r.optString("message"));Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();});}catch(Exception e){runOnUiThread(()->{startFarmButton.setEnabled(true);startFarmButton.setText("▶  BẮT ĐẦU FARM");status.setText("Lỗi bắt đầu farm: "+e.getMessage());Toast.makeText(this,"Lỗi: "+e.getMessage(),Toast.LENGTH_LONG).show();});}});}
    private void stopOne(int slot,String enteredUser){String user=enteredUser==null?"":enteredUser.trim();if(user.isEmpty())user=users[slot].getText().toString().trim();if(user.isEmpty()){Toast.makeText(this,"Slot chưa có username",Toast.LENGTH_SHORT).show();return;}final String target=user;io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("stop_one_json",target).toString());runOnUiThread(()->Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show());}catch(Exception e){runOnUiThread(()->Toast.makeText(this,"Lỗi: "+e.getMessage(),Toast.LENGTH_LONG).show());}});}
    private void stopBot(){ startService(new Intent(this,BotService.class).setAction(BotService.STOP)); status.setText("Đang dừng bot…"); }
    private void logoutAllSafe(){if(logoutAllPending)return;logoutAllPending=true;updateTeamActionButtons();handler.postDelayed(()->{logoutAllPending=false;updateTeamActionButtons();},185000);status.setText("Đang đưa cả team về SAFE rồi logout…");io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("safe_logout_all_json").toString());runOnUiThread(()->{status.setText(r.optString("message"));Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();});}catch(Exception e){runOnUiThread(()->{logoutAllPending=false;updateTeamActionButtons();status.setText("Lỗi LOGOUT ALL: "+e.getMessage());Toast.makeText(this,"Lỗi: "+e.getMessage(),Toast.LENGTH_LONG).show();});}});}
    private void autoLoadSkills(){for(int i=0;i<5;i++)if(enabled[i].isChecked()&&!users[i].getText().toString().trim().isEmpty())loadSkills(i);}
    private void refreshStatus(){ io.execute(() -> { try { JSONObject j=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("team_debug_json").toString());JSONObject flow=j.optJSONObject("coordination");StringBuilder out=new StringBuilder("Leader: ").append(j.optString("leader","—")).append('\n');if(flow!=null){out.append("Pha: ").append(flow.optString("dt_phase","—")).append(" • Phân khu: ").append(flow.optString("train_channel_manual","chưa chọn")).append('\n');if(flow.optBoolean("ui_dg_transition_pending"))out.append("⏳ Đợi team thoát Dị giới, sẵn sàng gom party\n");}JSONArray accounts=j.optJSONArray("accounts");if(accounts!=null)for(int i=0;i<accounts.length();i++){JSONObject a=accounts.getJSONObject(i);out.append(a.optBoolean("online")?"● ":"○ ").append(a.optString("name").isEmpty()?a.optString("user"):a.optString("name")).append(" • ").append(a.optString("area")).append(" • ").append(a.optString("activity")).append('\n');}out.append("\n── NHẬT KÝ CHUNG ──\n");JSONArray logs=j.optJSONArray("logs");if(logs!=null)for(int i=Math.max(0,logs.length()-100);i<logs.length();i++)out.append(logs.getString(i)).append('\n');String display=out.toString();runOnUiThread(()->status.setText(display));}catch(Exception e){runOnUiThread(()->status.setText("Không đọc được log team; thử lại sau."));} }); }

    @Override protected void onActivityResult(int requestCode,int resultCode,Intent data){super.onActivityResult(requestCode,resultCode,data);if(requestCode!=716||resultCode!=RESULT_OK||data==null||data.getData()==null)return;final android.net.Uri destination=data.getData();io.execute(()->{try{JSONObject snapshot=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("team_debug_json").toString());snapshot.put("app_version",getPackageManager().getPackageInfo(getPackageName(),0).versionName);byte[] bytes=snapshot.toString(2).getBytes(java.nio.charset.StandardCharsets.UTF_8);try(java.io.OutputStream output=getContentResolver().openOutputStream(destination,"w")){if(output==null)throw new java.io.IOException("Không mở được file");output.write(bytes);}runOnUiThread(()->Toast.makeText(this,"Đã xuất log team JSON",Toast.LENGTH_LONG).show());}catch(Exception e){runOnUiThread(()->Toast.makeText(this,"Xuất log thất bại: "+e.getMessage(),Toast.LENGTH_LONG).show());}}); }

    private void refreshMapSnapshot(){
        if(teamMapView==null||io.isShutdown()||mapRefreshPending)return;
        final boolean accountPage=currentPage==2&&accountManagerView!=null;
        final String focus=accountPage?accountManagerView.getSelectedUser():"";
        mapRefreshPending=true;
        try{io.execute(()->{try{
            JSONObject data=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("map_snapshot_json",focus).toString());
            if(data.optBoolean("ok"))runOnUiThread(()->{if(isFinishing()||isDestroyed())return;if(accountPage){if(currentPage!=2||!focus.equals(accountManagerView.getSelectedUser()))return;JSONObject maps=new JSONObject();try{maps.put(focus,data);}catch(Exception ignored){}accountManagerView.setMapSnapshots(maps);}else teamMapView.setSnapshot(data);});
        }catch(Exception ignored){}finally{mapRefreshPending=false;}});
        }catch(java.util.concurrent.RejectedExecutionException e){mapRefreshPending=false;}
    }
    private boolean allConfiguredAccountsOnline(){
        if(accountManagerView==null)return false;int configured=0;
        for(int slot=0;slot<5;slot++){
            String user=accountManagerView.configuredUser(slot);if(user.isEmpty())continue;configured++;
            boolean online=false;for(int i=0;i<bottomAccounts.length();i++){JSONObject a=bottomAccounts.optJSONObject(i);if(a!=null&&user.equals(a.optString("user"))){online=a.optBoolean("online");break;}}
            if(!online)return false;
        }
        return configured>0;
    }
    private void updateTeamActionButtons(){
        boolean anyOnline=false,anyConnecting=false;
        for(int i=0;i<bottomAccounts.length();i++){JSONObject a=bottomAccounts.optJSONObject(i);if(a!=null){anyOnline|=a.optBoolean("online");anyConnecting|=a.optBoolean("logging_in");}}
        if(!anyOnline&&!anyConnecting)logoutAllPending=false;
        if(loginAllButton!=null){loginAllButton.setEnabled(!loginAllPending&&!anyConnecting&&(accountManagerView==null||!accountManagerView.hasPendingLogin())&&!allConfiguredAccountsOnline());loginAllButton.setText(loginAllPending?"⌛ ĐANG LOGIN…":"🔑  LOGIN ALL");}
        if(logoutAllButton!=null){logoutAllButton.setVisibility(anyOnline?View.VISIBLE:View.GONE);logoutAllButton.setEnabled(!logoutAllPending);logoutAllButton.setText(logoutAllPending?"⌛ ĐANG VỀ SAFE…":"⏻  LOGOUT ALL");}
    }
    private void refreshAccountsDashboard(){if(accountManagerView==null)return;final boolean acknowledged=loginAllAcknowledged;io.execute(()->{try{JSONObject data=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("accounts_dashboard_json").toString());if(data.optBoolean("ok"))runOnUiThread(()->{bottomAccounts=data.optJSONArray("accounts");if(bottomAccounts==null)bottomAccounts=new JSONArray();accountManagerView.setData(data);if(loginAllPending&&acknowledged){loginAllPending=false;loginAllAcknowledged=false;accountManagerView.clearLoginPending("");}updateBottomNav();updateTeamActionButtons();});}catch(Exception ignored){}});}
    private void moveTeamFromMap(int x,int y){Toast.makeText(this,"Đã chọn X "+x+", Y "+y+" — đang tìm đường an toàn…",Toast.LENGTH_SHORT).show();io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("move_team_json",x,y).toString());runOnUiThread(()->{if(r.optBoolean("ok")){teamMapView.setRoute(r.optJSONArray("path"),r.optJSONArray("target"));if(accountManagerView!=null)accountManagerView.setMapRoute(r.optJSONArray("path"),r.optJSONArray("target"));}Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();});}catch(Exception e){runOnUiThread(()->Toast.makeText(this,"Lỗi di chuyển: "+e.getMessage(),Toast.LENGTH_LONG).show());}});}
    private void teleportOne(int slot,String user,int cityId){io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("teleport_city_one_json",user,cityId).toString());runOnUiThread(()->{accountManagerView.setActionMessage(slot,r.optString("message"),!r.optBoolean("ok"));Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();});}catch(Exception e){runOnUiThread(()->accountManagerView.setActionMessage(slot,"Lỗi teleport: "+e.getMessage(),true));}});}
    private void applyCombatSettings(int slot,String user,int petId,int charSkill,int petSkill,int hpPercent,int spPercent,boolean usePhucThan,boolean useDaiPhucThan,int charMobMin,int petMobMin,int petHpPercent,int petSpPercent,boolean useDgHoPhu,boolean autoBuyBaoHop){io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("apply_combat_settings_json",user,petId,charSkill,petSkill,hpPercent,spPercent,usePhucThan,charMobMin,petMobMin,useDaiPhucThan,petHpPercent,petSpPercent,useDgHoPhu,autoBuyBaoHop).toString());runOnUiThread(()->{accountManagerView.setActionMessage(slot,r.optString("message"),!r.optBoolean("ok"));Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();});}catch(Exception e){runOnUiThread(()->accountManagerView.setActionMessage(slot,"Lỗi cấu hình chiến đấu: "+e.getMessage(),true));}});}
    private void runAccountAction(String user,String action,JSONObject payload){io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("account_action_json",user,action,payload==null?"{}":payload.toString()).toString());runOnUiThread(()->{accountManagerView.setActionMessageForUser(user,r.optString("message"),!r.optBoolean("ok"));Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();refreshAccountsDashboard();});}catch(Exception e){runOnUiThread(()->{accountManagerView.setActionMessageForUser(user,"Lỗi thao tác: "+e.getMessage(),true);Toast.makeText(this,"Lỗi thao tác: "+e.getMessage(),Toast.LENGTH_LONG).show();});}});}
    private void applyChannelPolicy(boolean autoMode,int channel){io.execute(()->{try{JSONObject r=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("set_train_channel_policy_json",autoMode,channel).toString());runOnUiThread(()->{accountManagerView.setChannelPolicyResult(r.optBoolean("ok"),autoMode,channel,r.optString("message"));Toast.makeText(this,r.optString("message"),Toast.LENGTH_LONG).show();});}catch(Exception e){runOnUiThread(()->accountManagerView.setChannelPolicyResult(false,autoMode,channel,"Lỗi cấu hình phân khu: "+e.getMessage()));}});}

    private void updateFarmPoints(){
        if(maps.isEmpty()||mapSpinner.getSelectedItemPosition()<0)return;
        try{JSONArray pts=maps.get(mapSpinner.getSelectedItemPosition()).optJSONArray("mobs");List<String> labels=new ArrayList<>();labels.add("Tự chọn tọa độ bên dưới");if(pts!=null)for(int i=0;i<pts.length();i++){JSONArray p=pts.getJSONArray(i);labels.add("Điểm "+(i+1)+"  •  X "+p.getInt(0)+"  Y "+p.getInt(1));}farmPointSpinner.setAdapter(adapter(labels));farmX.setText("");farmY.setText("");farmPointSpinner.setSelection(pts!=null&&pts.length()>0?1:0);if(pts!=null&&pts.length()>0)applyFarmPoint(1);}catch(Exception e){status.setText("Không đọc được điểm farm: "+e.getMessage());}
    }
    private void applyFarmPoint(int pos){if(pos<=0||maps.isEmpty())return;try{JSONArray p=maps.get(mapSpinner.getSelectedItemPosition()).getJSONArray("mobs").getJSONArray(pos-1);farmX.setText(String.valueOf(p.getInt(0)));farmY.setText(String.valueOf(p.getInt(1)));}catch(Exception ignored){}}
    private void saveTrainSelection(){if(trainPrefs==null||maps.isEmpty()||mapSpinner==null)return;try{int mi=Math.max(0,mapSpinner.getSelectedItemPosition());trainPrefs.edit().putInt("map_id",maps.get(mi).optInt("id",0)).putInt("point",Math.max(0,farmPointSpinner.getSelectedItemPosition())).putInt("digioi_level",digioiLevelSpinner.getSelectedItemPosition()+1).putInt("x",num(farmX,0)).putInt("y",num(farmY,0)).apply();}catch(Exception ignored){}}
    private void restoreTrainSelection(){if(trainPrefs==null||maps.isEmpty())return;restoringTrainSelection=true;int savedMap=trainPrefs.getInt("map_id",0),mapPos=0;for(int i=0;i<maps.size();i++)if(maps.get(i).optInt("id",0)==savedMap){mapPos=i;break;}mapSpinner.setSelection(mapPos);digioiLevelSpinner.setSelection(Math.max(0,Math.min(14,trainPrefs.getInt("digioi_level",2)-1)));updateFarmPoints();final int savedPoint=trainPrefs.getInt("point",0),savedX=trainPrefs.getInt("x",0),savedY=trainPrefs.getInt("y",0);handler.post(()->{int max=farmPointSpinner.getAdapter()==null?0:farmPointSpinner.getAdapter().getCount()-1;farmPointSpinner.setSelection(Math.max(0,Math.min(savedPoint,max)));if(savedX>0)farmX.setText(String.valueOf(savedX));if(savedY>0)farmY.setText(String.valueOf(savedY));restoringTrainSelection=false;updateSelectionInfo();});}
private void updateSelectionInfo(){if(selectionInfo==null)return;String s=serverSpinner.getSelectedItem()==null?"—":serverSpinner.getSelectedItem().toString(),m=mapSpinner.getSelectedItem()==null?"—":mapSpinner.getSelectedItem().toString(),p=farmPointSpinner.getSelectedItem()==null?"Tự chọn":farmPointSpinner.getSelectedItem().toString();String mode=modeSpinner.getSelectedItem()==null?"—":modeSpinner.getSelectedItem().toString();selectionInfo.setText("Chế độ: "+mode+"\nĐã chọn: "+s+"  |  "+m+"  |  "+p+(modeSpinner.getSelectedItemPosition()==2?"\nDị giới "+digioiLevelSpinner.getSelectedItem()+" → chờ cả team hết giờ → ra bãi farm đã chọn. Chỉ chạy khi bấm BẮT ĐẦU FARM.":""));}
    private int selectedId(List<Integer> ids,Spinner spinner){int p=spinner.getSelectedItemPosition();return p>=0&&p<ids.size()?ids.get(p):0;}
    private void loadSkills(int slot){String user=users[slot].getText().toString().trim();if(user.isEmpty()){status.setText("Slot "+(slot+1)+": nhập username trước.");return;}status.setText("Đang lấy nhân vật, pet và skill của "+user+"…");io.execute(()->{try{JSONObject result=new JSONObject(Python.getInstance().getModule("agent_bridge").callAttr("skills_json",user).toString());if(!result.optBoolean("ok"))throw new Exception(result.optString("message"));JSONObject data=result.getJSONObject("data");JSONArray chars=data.optJSONArray("char"),pets=data.optJSONArray("pets"),activePet=data.optJSONArray("pet");List<String> cl=new ArrayList<>(),pl=new ArrayList<>();List<Integer>ci=new ArrayList<>(),pi=new ArrayList<>();cl.add("Nhân vật: Tự động");ci.add(0);pl.add("Pet: Tự động");pi.add(0);if(chars!=null)for(int i=0;i<chars.length();i++){JSONArray x=chars.getJSONArray(i);ci.add(x.getInt(0));cl.add(x.optString(1,"Skill "+x.getInt(0))+"  ["+x.getInt(0)+"]");}if(activePet!=null)for(int i=0;i<activePet.length();i++){JSONArray x=activePet.getJSONArray(i);pi.add(x.getInt(0));pl.add(x.optString(1,"Skill "+x.getInt(0))+"  ["+x.getInt(0)+"]");}StringBuilder info=new StringBuilder("Pet mang theo: ");if(pets==null||pets.length()==0)info.append("chưa có dữ liệu");else for(int i=0;i<pets.length();i++){JSONArray p=pets.getJSONArray(i);if(i>0)info.append(" • ");info.append(p.optString(1,"Pet")).append(p.getInt(0)==data.optInt("active")?" (đang dùng)":"");}runOnUiThread(()->{charSkillIds.set(slot,ci);petSkillIds.set(slot,pi);charSkills[slot].setAdapter(adapter(cl));petSkills[slot].setAdapter(adapter(pl));charSkills[slot].setSelection(0);petSkills[slot].setSelection(0);petInfo[slot].setText(info.toString());status.setText("Đã nạp "+(cl.size()-1)+" skill nhân vật và "+(pl.size()-1)+" skill pet cho "+user+".");});}catch(Exception e){runOnUiThread(()->status.setText("Chưa lấy được skill của "+user+": "+e.getMessage()+". Hãy chờ login xong rồi thử lại."));}});}

    private int num(EditText e,int d){try{return Integer.parseInt(e.getText().toString().trim());}catch(Exception x){return d;}}
    private LinearLayout column(){LinearLayout l=new LinearLayout(this);l.setOrientation(LinearLayout.VERTICAL);return l;} private LinearLayout row(){LinearLayout l=column();l.setOrientation(LinearLayout.HORIZONTAL);return l;}
    private TextView text(String s,int z,int color){TextView v=new TextView(this);v.setText(s);v.setTextSize(z);v.setTextColor(color);return v;} private TextView label(String s){TextView v=text(s,12,Color.LTGRAY);v.setPadding(0,dp(8),0,dp(4));return v;}
    private TextView section(String s){TextView v=text(s,14,BLUE);v.setTypeface(Typeface.DEFAULT_BOLD);v.setPadding(0,dp(20),0,dp(8));return v;}
    private EditText input(String hint,boolean password){EditText e=new EditText(this);e.setHint(hint);e.setHintTextColor(Color.rgb(145,163,186));e.setTextColor(Color.rgb(244,248,255));e.setSingleLine(true);if(password)e.setInputType(InputType.TYPE_CLASS_TEXT|InputType.TYPE_TEXT_VARIATION_PASSWORD);return e;}
    private Spinner spinner(){Spinner s=new Spinner(this);s.setBackgroundTintList(android.content.res.ColorStateList.valueOf(GOLD));return s;} private ArrayAdapter<String> adapter(List<String> x){return new ArrayAdapter<String>(this,android.R.layout.simple_spinner_dropdown_item,x){private TextView paint(View v,int color){TextView t=(TextView)v;t.setTextColor(color);t.setTextSize(14);t.setPadding(dp(12),dp(10),dp(12),dp(10));return t;}@Override public View getView(int p,View c,android.view.ViewGroup g){return paint(super.getView(p,c,g),Color.WHITE);}@Override public View getDropDownView(int p,View c,android.view.ViewGroup g){TextView t=paint(super.getDropDownView(p,c,g),Color.rgb(25,35,48));t.setBackgroundColor(Color.WHITE);return t;}};}
    private Button button(String s,int bg,int fg){Button b=new Button(this);b.setText(s);b.setAllCaps(false);b.setTextSize(13);b.setMinHeight(dp(52));android.graphics.drawable.GradientDrawable shape=new android.graphics.drawable.GradientDrawable();shape.setColor(Color.WHITE);shape.setCornerRadius(dp(12));b.setBackground(shape);b.setBackgroundTintList(new ColorStateList(new int[][]{new int[]{-android.R.attr.state_enabled},new int[]{}},new int[]{Color.rgb(37,45,58),bg}));b.setTextColor(new ColorStateList(new int[][]{new int[]{-android.R.attr.state_enabled},new int[]{}},new int[]{Color.rgb(120,132,149),fg}));return b;} private View space(int h){Space s=new Space(this);s.setLayoutParams(new LinearLayout.LayoutParams(1,dp(h)));return s;}
    private LinearLayout.LayoutParams matchWrap(){LinearLayout.LayoutParams p=new LinearLayout.LayoutParams(-1,-2);p.setMargins(0,dp(4),0,dp(4));return p;} private LinearLayout.LayoutParams weight(){LinearLayout.LayoutParams p=new LinearLayout.LayoutParams(0,-2,1);p.setMargins(dp(4),dp(8),dp(4),dp(8));return p;} private int dp(int n){return (int)(n*getResources().getDisplayMetrics().density+.5f);}
    @Override protected void onResume(){super.onResume();handler.removeCallbacks(poll);handler.removeCallbacks(mapPoll);handler.post(poll);handler.post(mapPoll);} @Override protected void onPause(){saveTrainSelection();handler.removeCallbacks(poll);handler.removeCallbacks(mapPoll);super.onPause();}
    @Override protected void onDestroy(){handler.removeCallbacksAndMessages(null);try{unregisterReceiver(resultReceiver);}catch(Exception ignored){}io.getQueue().clear();io.shutdownNow();super.onDestroy();}
}
