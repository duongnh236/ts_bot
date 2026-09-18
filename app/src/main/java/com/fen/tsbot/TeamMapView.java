package com.fen.tsbot;

import android.content.Context;
import android.graphics.*;
import android.view.View;
import android.view.MotionEvent;
import org.json.*;
import java.util.*;
import android.util.Base64;

public class TeamMapView extends View {
    private JSONObject snapshot = new JSONObject();
    private int terrainMap=-1;private byte[] terrainData=new byte[0];
    private final Paint p = new Paint(Paint.ANTI_ALIAS_FLAG);
    private final int bg=Color.rgb(7,16,29), grid=Color.rgb(27,49,72), gold=Color.rgb(229,184,83);
    private double loX,hiX,loY,hiY; private float plotLeft,plotRight,plotTop,plotBottom; private boolean hasBounds=false;
    private double tapX,tapY; private boolean hasTap=false; private JSONArray route=new JSONArray(); private OnMapTapListener tapListener;
    public interface OnMapTapListener{void onMapTap(int x,int y);}

    public TeamMapView(Context c){super(c);p.setTypeface(Typeface.create(Typeface.MONOSPACE,Typeface.NORMAL));setBackgroundColor(bg);}
    public void setOnMapTapListener(OnMapTapListener listener){tapListener=listener;}
    public synchronized void setSnapshot(JSONObject value){JSONObject next=value==null?new JSONObject():value;if(next.optInt("map")!=snapshot.optInt("map")||next.optInt("channel")!=snapshot.optInt("channel")){hasBounds=false;hasTap=false;route=new JSONArray();}snapshot=next;int map=next.optInt("map");if(terrainMap!=map||terrainData.length==0){JSONObject terrain=next.optJSONObject("collision");try{terrainData=terrain==null?new byte[0]:Base64.decode(terrain.optString("data",""),Base64.DEFAULT);}catch(Exception e){terrainData=new byte[0];}terrainMap=map;}invalidate();}
    public synchronized void setRoute(JSONArray value,JSONArray actualTarget){route=value==null?new JSONArray():value;if(actualTarget!=null){tapX=actualTarget.optDouble(0);tapY=actualTarget.optDouble(1);hasTap=true;}invalidate();}
    private void text(Canvas c,String s,float x,float y,float size,int color){p.setStyle(Paint.Style.FILL);p.setTextSize(size);p.setColor(color);c.drawText(s,x,y,p);}
    private void tag(Canvas c,String s,float x,float y,float size,int color){p.setTextSize(size);p.setStyle(Paint.Style.FILL);float width=p.measureText(s);p.setColor(Color.argb(205,5,12,22));c.drawRoundRect(x-4,y-size-3,x+width+4,y+5,5,5,p);text(c,s,x,y,size,color);}
    private void dot(Canvas c,float x,float y,float radius,int color){p.setStyle(Paint.Style.FILL);p.setColor(color);c.drawCircle(x,y,radius,p);p.setStyle(Paint.Style.STROKE);p.setStrokeWidth(2);p.setColor(Color.WHITE);c.drawCircle(x,y,radius,p);}
    private boolean blocked(byte[] data,int gh,int x,int y){if(x<0||y<0||x*gh+y>=data.length)return true;int v=data[x*gh+y]&255;return (v&1)!=0||(v&4)!=0;}

    @Override protected synchronized void onDraw(Canvas c){super.onDraw(c);float w=getWidth(),h=getHeight();
        String place=snapshot.optString("map_name","Chưa login");int map=snapshot.optInt("map"),channel=snapshot.optInt("channel");
        text(c,"🗺  "+place+"  •  Phân khu "+channel,18,30,16,gold);
        JSONArray team=snapshot.optJSONArray("team"),entities=snapshot.optJSONArray("entities"),safe=snapshot.optJSONArray("safe"),target=snapshot.optJSONArray("target");
        if(team==null||team.length()==0){text(c,"Login bot để xem tọa độ live.",18,65,14,Color.LTGRAY);return;}
        List<double[]> pts=new ArrayList<>();
        for(int i=0;i<team.length();i++){JSONObject x=team.optJSONObject(i);if(x!=null&&x.optInt("map")==map&&x.optInt("channel")==channel&&x.optInt("x")>0)pts.add(new double[]{x.optDouble("x"),x.optDouble("y")});}
        if(pts.isEmpty()){text(c,"Đang chờ server trả tọa độ…",18,65,14,Color.LTGRAY);return;}
        double[] anchor=pts.get(0);for(int i=0;i<team.length();i++){JSONObject a=team.optJSONObject(i);if(a!=null&&a.optString("user").equals(snapshot.optString("focus_user"))&&a.optInt("x")>0){anchor=new double[]{a.optDouble("x"),a.optDouble("y")};break;}}
        // Giu camera khi team/NPC thay doi; chi recenter khi focus toi gan bien.
        double cx=hasBounds?(loX+hiX)/2:Math.round(anchor[0]/400.0)*400.0,cy=hasBounds?(loY+hiY)/2:Math.round(anchor[1]/400.0)*400.0;
        if(Math.abs(anchor[0]-cx)>600||Math.abs(anchor[1]-cy)>600){cx=Math.round(anchor[0]/400.0)*400.0;cy=Math.round(anchor[1]/400.0)*400.0;}
        double minX=cx-800,maxX=cx+800,minY=cy-800,maxY=cy+800;
        float top=50,bottom=Math.max(top+100,h-125),left=18,right=w-18;
        p.setStrokeWidth(1);p.setColor(grid);for(int i=0;i<=8;i++){float x=left+(right-left)*i/8f,y=top+(bottom-top)*i/8f;c.drawLine(x,top,x,bottom,p);c.drawLine(left,y,right,y,p);}
        loX=minX;hiX=maxX;loY=minY;hiY=maxY;plotLeft=left;plotRight=right;plotTop=top;plotBottom=bottom;hasBounds=true;
        java.util.function.BiFunction<Double,Double,PointF> xy=(x,y)->new PointF((float)(left+(x-loX)/(hiX-loX)*(right-left)),(float)(top+(y-loY)/(hiY-loY)*(bottom-top)));
JSONObject collision=snapshot.optJSONObject("collision");if(collision!=null&&collision.optInt("grid_w")>0){try{int gw=collision.getInt("grid_w"),gh=collision.getInt("grid_h"),ox=collision.getInt("origin_x"),oy=collision.getInt("origin_y"),cell=collision.optInt("cell",20);byte[] data=terrainData;if(data.length<gw*gh)throw new IllegalArgumentException("Incomplete terrain");c.save();c.clipRect(left,top,right,bottom);for(int gx=0;gx<gw;gx++){double x0=ox+gx*cell,x1=x0+cell;if(x1<loX||x0>hiX)continue;for(int gy=0;gy<gh;gy++){double y0=oy+gy*cell,y1=y0+cell;if(y1<loY||y0>hiY)continue;int v=data[gx*gh+gy]&255;boolean wall=(v&1)!=0||(v&4)!=0,sea=(v&2)!=0;PointF a=xy.apply(x0,y0),b=xy.apply(x1,y1);p.setStyle(Paint.Style.FILL);p.setColor(wall?Color.argb(155,90,28,35):(sea?Color.argb(105,25,82,125):Color.argb(35,70,145,105)));c.drawRect(a.x,a.y,b.x,b.y,p);if(wall){p.setStyle(Paint.Style.STROKE);p.setStrokeWidth(1.5f);p.setColor(Color.rgb(225,75,75));if(!blocked(data,gh,gx-1,gy))c.drawLine(a.x,a.y,a.x,b.y,p);if(!blocked(data,gh,gx+1,gy))c.drawLine(b.x,a.y,b.x,b.y,p);if(!blocked(data,gh,gx,gy-1))c.drawLine(a.x,a.y,b.x,a.y,p);if(!blocked(data,gh,gx,gy+1))c.drawLine(a.x,b.y,b.x,b.y,p);}}}c.restore();}catch(Exception ignored){}}
        if(route!=null&&route.length()>1){p.setStyle(Paint.Style.STROKE);p.setStrokeWidth(5);p.setStrokeCap(Paint.Cap.ROUND);p.setStrokeJoin(Paint.Join.ROUND);p.setColor(Color.rgb(255,215,70));Path line=new Path();for(int i=0;i<route.length();i++){JSONArray q=route.optJSONArray(i);if(q==null)continue;PointF a=xy.apply(q.optDouble(0),q.optDouble(1));if(i==0)line.moveTo(a.x,a.y);else line.lineTo(a.x,a.y);}c.drawPath(line,p);p.setStrokeCap(Paint.Cap.BUTT);}
        if(safe!=null)for(int i=0;i<safe.length();i++){JSONArray q=safe.optJSONArray(i);if(q!=null){PointF a=xy.apply(q.optDouble(0),q.optDouble(1));p.setColor(Color.rgb(65,170,120));p.setStyle(Paint.Style.STROKE);p.setStrokeWidth(3);c.drawCircle(a.x,a.y,10,p);text(c,"SAFE",a.x+12,a.y,10,Color.rgb(100,220,160));}}
        JSONArray tq=snapshot.optJSONArray("target");if(tq!=null){PointF a=xy.apply(tq.optDouble(0),tq.optDouble(1));p.setColor(Color.RED);p.setStrokeWidth(3);c.drawLine(a.x-9,a.y-9,a.x+9,a.y+9,p);c.drawLine(a.x+9,a.y-9,a.x-9,a.y+9,p);text(c,"BÃI TRAIN",a.x+12,a.y,11,Color.rgb(255,120,100));}
        int mobCount=0,playerCount=0;if(entities!=null)for(int i=0;i<entities.length();i++){JSONObject e=entities.optJSONObject(i);if(e==null)continue;PointF a=xy.apply(e.optDouble("x"),e.optDouble("y"));boolean mob="mob".equals(e.optString("kind"));if(mob)mobCount++;else playerCount++;dot(c,a.x,a.y,mob?8:11,mob?Color.rgb(230,75,65):Color.rgb(180,105,245));String name=e.optString("name",mob?"Quái":"Người chơi");tag(c,(mob?"QUÁI • ":"NGƯỜI • ")+name,a.x+(mob?11:14),a.y-9,mob?11:12,mob?Color.rgb(255,185,155):Color.rgb(225,205,255));}
        int same=0,joined=0;for(int i=0;i<team.length();i++){JSONObject a=team.optJSONObject(i);if(a==null)continue;boolean here=a.optInt("map")==map&&a.optInt("channel")==channel;if(here)same++;if(a.optBoolean("in_party")||a.optBoolean("leader"))joined++;if(a.optInt("x")>0&&here){PointF q=xy.apply(a.optDouble("x"),a.optDouble("y"));boolean lead=a.optBoolean("leader"),qs=a.optBoolean("strategist");dot(c,q.x,q.y,lead?19:16,lead?gold:(qs?Color.rgb(75,220,145):Color.rgb(70,170,255)));String iv=a.isNull("int")?"?":String.valueOf(a.optInt("int"));tag(c,(lead?"LEADER • ":(qs?"QUÂN SƯ • ":"MEMBER • "))+a.optString("name")+" • INT "+iv+" ["+a.optInt("x")+","+a.optInt("y")+"]",q.x+22,q.y-12,14,Color.WHITE);}}
        if(hasTap){PointF q=xy.apply(tapX,tapY);p.setColor(Color.YELLOW);p.setStyle(Paint.Style.STROKE);p.setStrokeWidth(3);c.drawCircle(q.x,q.y,14,p);c.drawLine(q.x-20,q.y,q.x+20,q.y,p);c.drawLine(q.x,q.y-20,q.x,q.y+20,p);tag(c,"ĐÍCH CHẠM ["+(int)tapX+","+(int)tapY+"]",q.x+18,q.y-15,12,Color.YELLOW);}
        text(c,"Cùng map/khu: "+same+"/"+team.length()+"  •  Trong PT: "+joined+"/"+team.length()+"  •  Quái: "+mobCount+"  •  Người khác: "+playerCount,18,h-88,13,same==team.length()?Color.rgb(90,220,145):Color.rgb(255,135,95));
        text(c,"● Leader  ● Quân sư  ● Member  ● Người chơi  ● Quái  ■ Tường/Nước",18,h-58,11,Color.LTGRAY);
        text(c,"Chạm bản đồ: leader kéo nguyên PT • cập nhật 1 giây",18,h-30,11,Color.rgb(130,160,190));
    }
    @Override public boolean onTouchEvent(MotionEvent e){if(e.getAction()!=MotionEvent.ACTION_UP)return true;if(!hasBounds||e.getX()<plotLeft||e.getX()>plotRight||e.getY()<plotTop||e.getY()>plotBottom)return true;tapX=loX+(e.getX()-plotLeft)/(plotRight-plotLeft)*(hiX-loX);tapY=loY+(e.getY()-plotTop)/(plotBottom-plotTop)*(hiY-loY);hasTap=true;invalidate();if(tapListener!=null)tapListener.onMapTap((int)Math.round(tapX),(int)Math.round(tapY));return true;}
}
