
import org.apache.amaterasu.leader.container.dockerFile
import org.junit.Test

class DockerImageFileTest {


  @Test
  fun buildBasicImageTest() {
    val dockerFile = dockerFile {
      from("busybox")
      commands("COPY A to B","Volume A/B")
      entrypoint("java", "-jar", "DirBuster-0.12.jar", "-H")
    }

    println(dockerFile.compileImage())

  }

}