package com.fen.tsbot;
import android.app.*;import android.content.Intent;import android.os.IBinder;
import com.chaquo.python.PyObject;import com.chaquo.python.Python;import com.chaquo.python.android.AndroidPlatform;
import java.util.concurrent.ExecutorService;import java.util.concurrent.Executors;

public class BotService extends Service {
 public static final String START="com.fen.tsbot.START",STOP="com.fen.tsbot.STOP",RESULT="com.fen.tsbot.RESULT";
 private final ExecutorService worker=Executors.newSingleThreadExecutor();
 @Override public void onCreate(){super.onCreate();AssetInstaller.ensure(this);if(!Python.isStarted())Python.start(new AndroidPlatform(this));getSystemService(NotificationManager.class).createNotificationChannel(new NotificationChannel("bot","Bot đang chạy",NotificationManager.IMPORTANCE_LOW));}
 @Override public int onStartCommand(Intent intent,int flags,int startId){String action=intent==null?"":intent.getAction();if(START.equals(action)){startForeground(7,notification("Bot đang kết nối TS Online…"));String payload=intent.getStringExtra("payload");worker.execute(()->call("start_json",payload==null?"{}":payload));}else if(STOP.equals(action)){worker.execute(()->{call("stop_all",null);stopForeground(STOP_FOREGROUND_REMOVE);stopSelf();});}return START_NOT_STICKY;}
 private void call(String name,String arg){String value;try{PyObject m=Python.getInstance().getModule("agent_bridge");value=arg==null?m.callAttr(name).toString():m.callAttr(name,arg).toString();}catch(Throwable t){value="{\"ok\":false,\"message\":\""+t.getClass().getSimpleName()+": "+String.valueOf(t.getMessage()).replace("\"","'")+"\"}";}sendBroadcast(new Intent(RESULT).setPackage(getPackageName()).putExtra("json",value));}
 private Notification notification(String text){PendingIntent pi=PendingIntent.getActivity(this,0,new Intent(this,MainActivity.class),PendingIntent.FLAG_IMMUTABLE);return new Notification.Builder(this,"bot").setSmallIcon(R.drawable.ic_launcher).setContentTitle("aTSBot Android").setContentText(text).setContentIntent(pi).setOngoing(true).build();}
 @Override public IBinder onBind(Intent intent){return null;}@Override public void onDestroy(){worker.shutdownNow();super.onDestroy();}
}
