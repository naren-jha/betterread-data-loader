package com.njha.betterreaddataloader;

import com.njha.betterreaddataloader.author.Author;
import com.njha.betterreaddataloader.author.AuthorRepository;
import com.njha.betterreaddataloader.book.Book;
import com.njha.betterreaddataloader.book.BookRepository;
import com.njha.betterreaddataloader.connection.DataStaxAstraProperties;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
@Slf4j
public class BetterreadDataLoaderApplication {

	@Autowired
	private AuthorRepository authorRepository;

	@Autowired
	private BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadDataLoaderApplication.class, args);
	}

	@PostConstruct
	public void start() {
		//initAuthors();
		initWorks();
	}

	private void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject authorJsonObj = new JSONObject(jsonString); // https://www.baeldung.com/java-org-json

				Author author = Author.builder()
						.id(authorJsonObj.getString("key").replace("/authors/", ""))
						.name(authorJsonObj.optString("name"))
						.personalName(authorJsonObj.optString("personal_name"))
						.build();
				log.info("saving author {}...", author.getName());
				authorRepository.save(author);
			});
		}
		catch (IOException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private void initWorks() {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		Path path = Paths.get(worksDumpLocation);

		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject bookJsonObj = new JSONObject(jsonString); // https://www.baeldung.com/java-org-json

				Book book = Book.builder()
						.id(bookJsonObj.getString("key").replace("/works/", ""))
						.name(bookJsonObj.optString("title"))
						.build();

				JSONObject bookDescJsonObj = bookJsonObj.optJSONObject("description");
				if (Objects.nonNull(bookDescJsonObj)) {
					book.setDescription(bookDescJsonObj.optString("value"));
				}

				JSONObject bookPublishedDtJsonObj = bookJsonObj.optJSONObject("created");
				if (Objects.nonNull(bookPublishedDtJsonObj)) {
					String dateStr = bookPublishedDtJsonObj.optString("value");
					book.setPublishedDate(LocalDate.parse(dateStr, formatter));
				}

				JSONArray bookCoversJsonArray = bookJsonObj.optJSONArray("covers");
				if (Objects.nonNull(bookCoversJsonArray)) {
					List<String> coverIds = new ArrayList<>();
					for (int i = 0; i < bookCoversJsonArray.length(); ++i) {
						int coverId = bookCoversJsonArray.getInt(i);
						coverIds.add(String.valueOf(coverId));
					}
					book.setCoverIds(coverIds);
				}

				JSONArray bookAuthorsJsonArray = bookJsonObj.optJSONArray("authors");
				if (Objects.nonNull(bookAuthorsJsonArray)) {
					List<String> authorIds = new ArrayList<>();
					for (int i = 0; i < bookAuthorsJsonArray.length(); ++i) {
						String authorId = bookAuthorsJsonArray.getJSONObject(i).getJSONObject("author").getString("key")
								.replace("/authors/", "");
						authorIds.add(authorId);
					}
					book.setAuthorIds(authorIds);

					// get author names from cassandra author_by_id table
					List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
							.map(authorOptional -> authorOptional.isPresent() ? authorOptional.get().getName() : "Unknown Author")
							.collect(Collectors.toList());
					book.setAuthorNames(authorNames);
				}

				log.info("saving book {}...", book.getName());
				bookRepository.save(book);
			});
		}
		catch (IOException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}

}
