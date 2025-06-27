package in.rcard.sc;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  record GitHubUser(User user, List<Repository> repositories) {}

  record User(UserId userId, UserName name, Email email) {}

  record UserId(long value) {}

  record UserName(String value) {}

  record Email(String value) {}

  record Repository(String name, Visibility visibility, URI uri) {}

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

  static void delay(Duration duration) throws InterruptedException {
    Thread.sleep(duration);
  }

  static class GitHubRepository implements FindUserByIdPort, FindRepositoriesByUserIdPort {

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
  }

  interface FindGitHubUserUseCase {
    GitHubUser findGitHubUser(UserId userId) throws InterruptedException;
  }

  interface FindGitHubUsersUseCase {
    List<GitHubUser> findGitHubUsers(List<UserId> userIds) throws InterruptedException;
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

        return scope.join().map(StructuredTaskScope.Subtask::get).toList();
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

  void main() throws InterruptedException {
    var gitHubRepository = new GitHubRepository();
    var findGitHubUserService = new FindGitHubUserService(gitHubRepository, gitHubRepository);
    var findGitHubUsersService = new FindGitHubUsersService(findGitHubUserService);

    findGitHubUsersService.findGitHubUsers(List.of(new UserId(1L), new UserId(2L)));
  }
}
