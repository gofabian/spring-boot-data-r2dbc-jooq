package gofabian.db;

import java.util.Objects;

public class BookPojo {

    private Long id;
    private String name;

    public BookPojo() {
    }

    public BookPojo(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "BookPojo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BookPojo bookPojo = (BookPojo) o;
        return Objects.equals(id, bookPojo.id) &&
                Objects.equals(name, bookPojo.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }
}
