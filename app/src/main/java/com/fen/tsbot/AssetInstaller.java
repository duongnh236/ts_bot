package com.fen.tsbot;

import android.content.Context;
import android.content.res.AssetManager;
import java.io.*;

final class AssetInstaller {
    static void ensure(Context context) {
        File root = new File(context.getFilesDir(), "bot_bundle/current/data");
        File marker = new File(root, ".installed-v1");
        if (marker.isFile()) return;
        try {
            copyTree(context.getAssets(), "train_bot_data", root);
            root.mkdirs();
            marker.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException("Không thể cài dữ liệu bot", e);
        }
    }

    private static void copyTree(AssetManager am, String assetPath, File target) throws IOException {
        String[] children = am.list(assetPath);
        if (children != null && children.length > 0) {
            if (!target.isDirectory() && !target.mkdirs()) throw new IOException("mkdir " + target);
            for (String child : children) copyTree(am, assetPath + "/" + child, new File(target, child));
            return;
        }
        File parent = target.getParentFile();
        if (parent != null) parent.mkdirs();
        try (InputStream in = am.open(assetPath); OutputStream out = new FileOutputStream(target)) {
            byte[] buf = new byte[65536]; int count;
            while ((count = in.read(buf)) != -1) out.write(buf, 0, count);
        }
    }
    private AssetInstaller() {}
}
