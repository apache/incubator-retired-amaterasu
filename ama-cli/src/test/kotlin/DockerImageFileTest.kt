
import org.apache.amaterasu.ama.cli.container.dockerFile
import org.junit.Test
import java.io.File
import kotlin.test.assertEquals

class DockerImageFileTest {


  @Test
  fun buildBasicImageTest() {
    //given
    val expectedDockerFile = """FROM busybox
        COPY A to B
        Volume A/B
        ENTRYPOINT [java, -jar, somejar.jar, -H]""".trimIndent()

    val dockerFile = dockerFile {
      from("busybox")
      commands("COPY A to B", "Volume A/B")
      entrypoint("java", "-jar", "somejar.jar", "-H")
    }

    //when
    dockerFile.createImageFile()

    //then
    assertEquals(expectedDockerFile, File("Dockerfile").readLines().joinToString(separator = "\n"))

    File("Dockerfile").delete()
  }

}