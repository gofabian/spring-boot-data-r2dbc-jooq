package gofabian.example;

import java.time.LocalDateTime;
import java.util.Objects;

public class BookPojo {

    private Long id;
    private String name;
    private LocalDateTime timestamp;

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

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "BookPojo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", timestamp=" + timestamp +
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
