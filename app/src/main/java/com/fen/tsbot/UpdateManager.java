package com.fen.tsbot;

import android.app.Activity;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.pm.PackageInstaller;
import android.content.pm.PackageInfo;
import android.net.Uri;
import android.os.Build;
import android.provider.Settings;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class UpdateManager {
    private static final String LATEST_RELEASE="https://api.github.com/repos/duongnh236/ts_bot/releases/latest";
    private static final String INSTALL_ACTION="com.fen.tsbot.UPDATE_INSTALL_RESULT";
    private static final ExecutorService IO=Executors.newSingleThreadExecutor();

    static final class Release {
        final int versionCode; final String versionName; final String apkUrl;
        Release(int code,String name,String url){versionCode=code;versionName=name;apkUrl=url;}
    }
    interface Listener {
        void onStatus(String message);
        void onUpToDate(String version);
        void onUpdate(Release release);
        void onError(String message);
    }
    private UpdateManager(){}

    static void checkLatest(Context context,Listener listener){
        IO.execute(()->{
            HttpURLConnection connection=null;
            try{
                connection=open(LATEST_RELEASE);
                int status=connection.getResponseCode();
                if(status!=200)throw new Exception("GitHub trả mã "+status+(status==404?" (repo chưa có Release)":""));
                JSONObject root=new JSONObject(readText(connection.getInputStream()));
                String tag=root.optString("tag_name","");
                int remoteCode=versionCodeFromTag(tag);
                if(remoteCode<=0)throw new Exception("Tag Release phải có dạng v69, v70…");
                JSONArray assets=root.optJSONArray("assets");String apkUrl=null;
                if(assets!=null)for(int i=0;i<assets.length();i++){
                    JSONObject asset=assets.optJSONObject(i);
                    if(asset!=null&&asset.optString("name","").toLowerCase(Locale.ROOT).endsWith(".apk")){
                        apkUrl=asset.optString("browser_download_url","");if(!apkUrl.isEmpty())break;
                    }
                }
                if(remoteCode<=currentVersionCode(context))listener.onUpToDate(tag);
                else if(apkUrl==null)throw new Exception("Release "+tag+" chưa đính kèm file APK");
                else listener.onUpdate(new Release(remoteCode,tag,apkUrl));
            }catch(Exception e){listener.onError(safeMessage(e));}
            finally{if(connection!=null)connection.disconnect();}
        });
    }

    static void downloadAndInstall(Activity activity,Release release,Listener listener){
        if(Build.VERSION.SDK_INT>=Build.VERSION_CODES.O&&!activity.getPackageManager().canRequestPackageInstalls()){
            listener.onError("Hãy bật ‘Cho phép từ nguồn này’, rồi bấm Kiểm tra cập nhật lại");
            activity.startActivity(new Intent(Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES,Uri.parse("package:"+activity.getPackageName())));
            return;
        }
        listener.onStatus("Đang tải "+release.versionName+"…");
        IO.execute(()->{
            HttpURLConnection connection=null;PackageInstaller.Session session=null;File downloaded=null;
            try{
                connection=open(release.apkUrl);connection.setInstanceFollowRedirects(true);
                int status=connection.getResponseCode();
                if(status<200||status>=300)throw new Exception("Tải APK thất bại, mã "+status);
                long total=connection.getContentLengthLong();
                downloaded=File.createTempFile("tsbot-update-",".apk",activity.getCacheDir());
                long done=0,lastPercent=-1;
                try(InputStream input=new BufferedInputStream(connection.getInputStream());OutputStream output=new FileOutputStream(downloaded)){
                    byte[] buffer=new byte[65536];int count;
                    while((count=input.read(buffer))!=-1){output.write(buffer,0,count);done+=count;if(total>0){long percent=done*100/total;if(percent>=lastPercent+5){lastPercent=percent;listener.onStatus("Đang tải "+release.versionName+": "+percent+"%");}}}
                    output.flush();
                }
                if(done==0)throw new Exception("File APK tải về rỗng");
                if(total>0&&done!=total)throw new Exception("APK tải chưa đủ dung lượng ("+done+" / "+total+" bytes); hãy thử lại");
                PackageInfo archive=activity.getPackageManager().getPackageArchiveInfo(downloaded.getAbsolutePath(),0);
                if(archive==null)throw new Exception("File Release không phải APK hợp lệ");
                long archiveCode=Build.VERSION.SDK_INT>=28?archive.getLongVersionCode():archive.versionCode;
                if(!activity.getPackageName().equals(archive.packageName))throw new Exception("APK Release sai package: "+archive.packageName);
                if(archiveCode!=release.versionCode)throw new Exception("Release "+release.versionName+" nhưng APK bên trong là versionCode "+archiveCode+". Hãy đính kèm đúng APK v"+release.versionCode);
                PackageInstaller installer=activity.getPackageManager().getPackageInstaller();
                PackageInstaller.SessionParams params=new PackageInstaller.SessionParams(PackageInstaller.SessionParams.MODE_FULL_INSTALL);
                params.setAppPackageName(activity.getPackageName());
                int sessionId=installer.createSession(params);session=installer.openSession(sessionId);
                // PackageInstaller.fsync must receive the exact stream returned by openWrite.
                // Wrapping it in BufferedOutputStream causes Android's "Unrecognized stream".
                total=downloaded.length();
                try(InputStream input=new BufferedInputStream(new FileInputStream(downloaded));OutputStream output=session.openWrite("update.apk",0,total)){
                    byte[] buffer=new byte[65536];int count;
                    while((count=input.read(buffer))!=-1)output.write(buffer,0,count);
                    output.flush();session.fsync(output);
                }
                BroadcastReceiver receiver=new BroadcastReceiver(){@Override public void onReceive(Context context,Intent intent){
                    int result=intent.getIntExtra(PackageInstaller.EXTRA_STATUS,PackageInstaller.STATUS_FAILURE);
                    if(result==PackageInstaller.STATUS_PENDING_USER_ACTION){Intent confirm=intent.getParcelableExtra(Intent.EXTRA_INTENT);if(confirm!=null){confirm.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);context.startActivity(confirm);}}
                    else if(result==PackageInstaller.STATUS_SUCCESS){listener.onStatus("Đã cập nhật thành công");try{context.unregisterReceiver(this);}catch(Exception ignored){}}
                    else{listener.onError("Android không cài được APK: "+intent.getStringExtra(PackageInstaller.EXTRA_STATUS_MESSAGE));try{context.unregisterReceiver(this);}catch(Exception ignored){}}
                }};
                IntentFilter filter=new IntentFilter(INSTALL_ACTION);
                if(Build.VERSION.SDK_INT>=33)activity.registerReceiver(receiver,filter,Context.RECEIVER_NOT_EXPORTED);else activity.registerReceiver(receiver,filter);
                Intent callback=new Intent(INSTALL_ACTION).setPackage(activity.getPackageName());
                PendingIntent pending=PendingIntent.getBroadcast(activity,sessionId,callback,PendingIntent.FLAG_UPDATE_CURRENT|PendingIntent.FLAG_MUTABLE);
                listener.onStatus("Đã tải xong • chờ Android xác nhận cài đặt");
                session.commit(pending.getIntentSender());session.close();session=null;
            }catch(Exception e){listener.onError(safeMessage(e));if(session!=null)try{session.abandon();}catch(Exception ignored){}}
            finally{if(session!=null)try{session.close();}catch(Exception ignored){}if(connection!=null)connection.disconnect();if(downloaded!=null&&!downloaded.delete())downloaded.deleteOnExit();}
        });
    }

    static String currentVersionName(Context context){try{return context.getPackageManager().getPackageInfo(context.getPackageName(),0).versionName;}catch(Exception e){return "?";}}
    private static long currentVersionCode(Context context){try{android.content.pm.PackageInfo info=context.getPackageManager().getPackageInfo(context.getPackageName(),0);return Build.VERSION.SDK_INT>=28?info.getLongVersionCode():info.versionCode;}catch(Exception e){return 0;}}

    private static HttpURLConnection open(String url)throws Exception{HttpURLConnection c=(HttpURLConnection)new URL(url).openConnection();c.setConnectTimeout(15000);c.setReadTimeout(30000);c.setRequestProperty("Accept","application/vnd.github+json");c.setRequestProperty("User-Agent","aTSBot-Android-Updater");return c;}
    private static String readText(InputStream input)throws Exception{try(InputStream in=input){byte[] b=new byte[16384];StringBuilder s=new StringBuilder();int n;while((n=in.read(b))!=-1)s.append(new String(b,0,n,"UTF-8"));return s.toString();}}
    private static int versionCodeFromTag(String tag){String d=tag==null?"":tag.replaceAll("[^0-9]","");if(d.isEmpty())return 0;try{return Integer.parseInt(d);}catch(Exception ignored){return 0;}}
    private static String safeMessage(Exception e){String m=e.getMessage();return m==null||m.trim().isEmpty()?e.getClass().getSimpleName():m;}
}
