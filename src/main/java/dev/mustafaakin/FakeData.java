package dev.mustafaakin;

public class FakeData {
    private long id;
    private double value;

    public long getId() {
        return id;
    }

    public FakeData setId(long id) {
        this.id = id;
        return this;
    }

    public double getValue() {
        return value;
    }

    public FakeData setValue(double value) {
        this.value = value;
        return this;
    }
}