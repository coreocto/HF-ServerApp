package org.coreocto.dev.hf.serverapp.bean;

public class DocInfo {
    private String name;
    private int type;
    private String feiv;

    public String getFeiv() {
        return feiv;
    }

    public void setFeiv(String feiv) {
        this.feiv = feiv;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
