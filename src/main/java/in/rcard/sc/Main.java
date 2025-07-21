package in.rcard.sc;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.FailedException;
import java.util.concurrent.StructuredTaskScope.Joiner;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ALL")
public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  record GitHubUser(User user, List<Repository> repositories) {}

  record User(UserId userId, UserName name, Email email) {}

  record UserId(long value) {}

  record UserName(String value) {}

  record Email(String value) {}

  record Repository(String name, Visibility visibility, URI uri) {}

  record RepoStructure(Repository repository, List<String> files) {}

  enum Visibility {
    PUBLIC,
    PRIVATE
  }

  interface FindUserByIdPort {
    User findUser(UserId userId) throws InterruptedException;
  }

  interface FindRepositoriesByUserIdPort {
    List<Repository> findRepositories(UserId userId) throws InterruptedException;
  }

  interface FindRepositoriesByUserIdListPort {
    Map<UserId, List<Repository>> findRepositories(List<UserId> userIds)
        throws InterruptedException;
  }

  interface FindAllRepositoryFilesPort {
    List<String> findAllFiles(Repository repository) throws InterruptedException;
  }

  static void delay(Duration duration) throws InterruptedException {
    Thread.sleep(duration);
  }

  @SuppressWarnings("preview")
  static class GitHubRepository
      implements FindUserByIdPort,
          FindRepositoriesByUserIdPort,
          FindRepositoriesByUserIdListPort,
          FindAllRepositoryFilesPort {

    @Override
    public User findUser(UserId userId) throws InterruptedException {
      LOGGER.info("Finding user with id '{}'", userId);
      delay(Duration.ofMillis(500L));
      //      throw new RuntimeException("Error finding user with id '%s'".formatted(userId));
      LOGGER.info("User '{}' found", userId);
      return new User(userId, new UserName("rcardin"), new Email("rcardin@rockthejvm.com"));
    }

    @Override
    public List<Repository> findRepositories(UserId userId) throws InterruptedException {
      LOGGER.info("Finding repositories for user with id '{}'", userId);
      delay(Duration.ofSeconds(1L));
      LOGGER.info("Repositories found for user '{}'", userId);
      return List.of(
          new Repository(
              "raise4s", Visibility.PUBLIC, URI.create("https://github.com/rcardin/raise4s")),
          new Repository(
              "sus4s", Visibility.PUBLIC, URI.create("https://github.com/rcardin/sus4s")));
    }

    @Override
    public Map<UserId, List<Repository>> findRepositories(List<UserId> userIds)
        throws InterruptedException {
      var repositoriesByUserId = new HashMap<UserId, List<Repository>>();
      try (var scope = StructuredTaskScope.open(Joiner.awaitAll())) {
        userIds.forEach(
            userId -> {
              scope.fork(
                  () -> {
                    if (userId.equals(new UserId(42))) {
                      throw new RuntimeException(
                          "Network error while finding repositories for user '%s'"
                              .formatted(userId));
                    }
                    final List<Repository> repositories = findRepositories(userId);
                    repositoriesByUserId.put(userId, repositories);
                    return repositories;
                  });
            });
        scope.join();
      }
      return repositoriesByUserId;
    }

    @Override
    public List<String> findAllFiles(Repository repository) throws InterruptedException {
      LOGGER.info("Finding files for repository '{}'", repository.name);
      switch (repository.name) {
        case "raise4s" -> {
          delay(Duration.ofMillis(500L));
          return List.of("README.md", "src/main/scala/in/rcard/raise4s/Main.scala");
        }
        case "sus4s" -> {
          delay(Duration.ofMillis(100L));
          return List.of("src/main/scala/in/rcard/sus4s/Main.scala");
        }
        case "yaes" -> {
          delay(Duration.ofMillis(700L));
          return List.of("src/test/scala/in/rcard/yaes/MainTest.scala");
        }
        default -> {
          delay(Duration.ofMillis(250L));
          throw new RuntimeException("Unknown repository '%s'".formatted(repository.name));
        }
      }
    }
  }

  interface FindGitHubUserUseCase {
    GitHubUser findGitHubUser(UserId userId) throws InterruptedException;
  }

  interface FindGitHubUsersUseCase {
    List<GitHubUser> findGitHubUsers(List<UserId> userIds) throws InterruptedException;
  }

  interface FirstRepositoryByFileNameUseCase {
    Optional<Repository> firstRepositoryByFileName(
        List<Repository> repositories, String fileNameToMatch) throws InterruptedException;
  }

  static class FirstRepositoryByFileNameService implements FirstRepositoryByFileNameUseCase {

    private final FindAllRepositoryFilesPort findAllFilesPort;

    FirstRepositoryByFileNameService(FindAllRepositoryFilesPort findAllFilesPort) {
      this.findAllFilesPort = findAllFilesPort;
    }

    @Override
    public Optional<Repository> firstRepositoryByFileName(
        List<Repository> repositories, String fileNameToMatch) throws InterruptedException {

      class CancelIfFound implements Predicate<Subtask<? extends RepoStructure>> {

        @Override
        public boolean test(Subtask<? extends RepoStructure> subtask) {
          return subtask.state() == Subtask.State.SUCCESS
              && subtask.get().files().contains(fileNameToMatch);
        }
      }

      var cancelIfFound = new CancelIfFound();

      try (var scope = StructuredTaskScope.open(Joiner.<RepoStructure>allUntil(cancelIfFound))) {
        repositories.forEach(
            repository ->
                scope.fork(
                    () -> {
                      final List<String> files = findAllFilesPort.findAllFiles(repository);
                      LOGGER.info(
                          "Found {} files for repository '{}'", files.size(), repository.name);
                      return new RepoStructure(repository, files);
                    }));

        return scope
            .join()
            .filter(subtask -> subtask.state() == Subtask.State.SUCCESS)
            .findFirst()
            .map(subtask -> subtask.get().repository());
      }
    }
  }

  @SuppressWarnings("preview")
  static class FindGitHubUsersService implements FindGitHubUsersUseCase {

    private final FindGitHubUserUseCase findGitHubUser;

    FindGitHubUsersService(FindGitHubUserUseCase findGitHubUser) {
      this.findGitHubUser = findGitHubUser;
    }

    @Override
    public List<GitHubUser> findGitHubUsers(List<UserId> userIds) throws InterruptedException {
      try (var scope = StructuredTaskScope.open(Joiner.<GitHubUser>allSuccessfulOrThrow())) {
        userIds.forEach(userId -> scope.fork(() -> findGitHubUser.findGitHubUser(userId)));

        return scope.join().map(Subtask::get).toList();
      }
    }
  }

  @SuppressWarnings("preview")
  static class FindGitHubUserService implements FindGitHubUserUseCase {
    private final FindUserByIdPort findUserByIdPort;
    private final FindRepositoriesByUserIdPort findRepositoriesByUserIdPort;

    public FindGitHubUserService(
        FindUserByIdPort findUserByIdPort,
        FindRepositoriesByUserIdPort findRepositoriesByUserIdPort) {
      this.findUserByIdPort = findUserByIdPort;
      this.findRepositoriesByUserIdPort = findRepositoriesByUserIdPort;
    }

    @Override
    public GitHubUser findGitHubUser(UserId userId) throws InterruptedException {
      try (var scope = StructuredTaskScope.open()) {

        var user = scope.fork(() -> findUserByIdPort.findUser(userId));
        var repositories = scope.fork(() -> findRepositoriesByUserIdPort.findRepositories(userId));

        scope.join();

        return new GitHubUser(user.get(), repositories.get());
      }
    }
  }

  static class FindRepositoriesByUserIdCache implements FindRepositoriesByUserIdPort {

    private final Map<UserId, List<Repository>> cache;

    public FindRepositoriesByUserIdCache(FindRepositoriesByUserIdListPort findRepositories)
        throws InterruptedException {
      this.cache =
          findRepositories.findRepositories(List.of(new UserId(1), new UserId(2), new UserId(42L)));
    }

    @Override
    public List<Repository> findRepositories(UserId userId) throws InterruptedException {
      // Simulates access to a distributed cache (Redis?)
      delay(Duration.ofMillis(100L));
      final List<Repository> repositories = cache.get(userId);
      if (repositories == null) {
        LOGGER.info("No cached repositories found for user with id '{}'", userId);
        throw new NoSuchElementException(
            "No cached repositories found for user with id '%s'".formatted(userId));
      }
      return repositories;
    }

    public void addToCache(UserId userId, List<Repository> repositories)
        throws InterruptedException {
      // Simulates access to a distributed cache (Redis?)
      delay(Duration.ofMillis(100L));
      cache.put(userId, repositories);
    }
  }

  @SuppressWarnings("preview")
  static class GitHubCachedRepository implements FindRepositoriesByUserIdPort {

    private final FindRepositoriesByUserIdPort repository;
    private final FindRepositoriesByUserIdCache cache;

    GitHubCachedRepository(
        FindRepositoriesByUserIdPort repository, FindRepositoriesByUserIdCache cache) {
      this.repository = repository;
      this.cache = cache;
    }

    @Override
    public List<Repository> findRepositories(UserId userId)
        throws InterruptedException, FailedException {
      try (var scope =
          StructuredTaskScope.open(Joiner.<List<Repository>>anySuccessfulResultOrThrow())) {
        scope.fork(() -> cache.findRepositories(userId));
        scope.fork(
            () -> {
              final List<Repository> repositories = repository.findRepositories(userId);
              cache.addToCache(userId, repositories);
              return repositories;
            });
        return scope.join();
      }
    }
  }

  void main() throws InterruptedException {
    final FirstRepositoryByFileNameUseCase useCase =
        new FirstRepositoryByFileNameService(new GitHubRepository());

    final Optional<Repository> maybeRepoWithReadme =
        useCase.firstRepositoryByFileName(
            List.of(
                new Repository(
                    "raise4s", Visibility.PUBLIC, URI.create("https://github.com/rcardin/raise4s")),
                new Repository(
                    "sus4s", Visibility.PUBLIC, URI.create("https://github.com/rcardin/sus4s")),
                new Repository(
                    "yaes", Visibility.PUBLIC, URI.create("https://github.com/rcardin/yaes")),
                new Repository(
                    "kafkaesque",
                    Visibility.PUBLIC,
                    URI.create("https://github.com/rcardin/kafkaesque"))),
            "README.md");

    LOGGER.info(
        "First repository with 'README.md': {}",
        maybeRepoWithReadme.map(Repository::name).orElse("Not found"));
  }
}
