public class Movie {
    private String line;

    public Movie(String line) {
        this.line = line;
    }

    public boolean isDrama() {
        return line.contains("Drama");
    }

    @Override
    public String toString() {
        return line;
    }

    public boolean isGenre(String genre) {
        return line.contains(genre);
    }
}
