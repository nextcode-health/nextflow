package nextflow.cloud.gce;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.*
import nextflow.exception.AbortOperationException

import java.security.GeneralSecurityException;
import java.util.*;

/**
 * Helper class for Google Compute Engine
 *
 * @author Vilmundur Pálmason <vilmundur@wuxinextcode.com>
 */
class GceApiHelper {
    private static final String PROJECT_PREFIX = "https://www.googleapis.com/compute/v1/projects/";
    final String project
    final String zone
    private Compute compute
    def random = new Random()

    GceApiHelper(String project, String zone) throws IOException, GeneralSecurityException {
        this.project = project ?: readProject()
        this.zone = zone ?: readZone()
        this.compute = createComputeService()
    }

    Compute compute() {
        compute;
    }

    Compute createComputeService() throws IOException, GeneralSecurityException {
        def httpTransport = GoogleNetHttpTransport.newTrustedTransport()
        def jsonFactory = JacksonFactory.getDefaultInstance()

        def credential = GoogleCredential.getApplicationDefault()

        if (credential.createScopedRequired()) {
            credential =
                    credential.createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"))
        }

        new Compute.Builder(httpTransport, jsonFactory, credential)
                .setApplicationName("NextCode-Experiments/0.1")
                .build()
    }

    String projectZonePrefix() {
        "${PROJECT_PREFIX}$project/zones/$zone/"
    }

    /**
     * Full name of machine type
     * @param shortName Short name such as "n1-standard-1"
     * @return Fully qualifie machine type
     */
    String instanceType(String shortName) {
        "${projectZonePrefix()}machineTypes/$shortName"
    }

    /**
     * Full name of image.
     * @param imagePath including image project (e.g. "debian-cloud/global/images/debian-7-wheezy-v20150710" )
     * @return Fully qualified image name
     */
    String imageName(String imagePath) {
        PROJECT_PREFIX + imagePath
    }

    AttachedDisk setBootDisk(Instance instance, String imagePath) {
        def disk = new AttachedDisk()
        disk.setBoot(true)
        disk.setAutoDelete(true)
        disk.setType("PERSISTENT")
        def params = new AttachedDiskInitializeParams()
        // Assign the Persistent Disk the same name as the VM Instance.
        if (instance.getName() != null) {
            params.setDiskName(instance.getName())
        }
        // Specify the source operating system machine image to be used by the VM Instance.
        params.setSourceImage(imageName(imagePath))
        // Specify the disk type as Standard Persistent Disk
        params.setDiskType(projectZonePrefix() + "diskTypes/pd-standard")
        disk.setInitializeParams(params)
        instance.setDisks(Collections.singletonList(disk))
        disk
    }

    NetworkInterface setNetworkInterface(Instance inst) {
        def ifc = new NetworkInterface()
        ifc.setNetwork("${PROJECT_PREFIX}${project}/global/networks/default")
        List<AccessConfig> configs = []
        def config = new AccessConfig()
        config.setType("ONE_TO_ONE_NAT")
        config.setName("External NAT")
        configs.add(config)
        ifc.setAccessConfigs(configs)
        inst.setNetworkInterfaces(Collections.singletonList(ifc))
        ifc
    }

    String randomName(String baseName) {
        baseName + randomName()
    }

    String randomName() {
        def bytes = new byte[5]
        random.nextBytes(bytes)
        new BigInteger(bytes).abs().toString(16);
    }

    Metadata createMetadata(String... tagVal) {
        def metadata = new Metadata();

        List<Metadata.Items> items = []
        for (int i = 0; i < tagVal.length - 1; i += 2) {
            Metadata.Items it = new Metadata.Items()
            it.set(tagVal[i], tagVal[i + 1])
            items.add(it)
        }
        metadata.setItems(items)
        return metadata
    }

    /**
     * Block until all operations are complete or if any results in an error.
     */
    Operation.Error blockUntilComplete(Iterable<Operation> ops, long timeoutMs) throws InterruptedException, IOException {
        long start = System.currentTimeMillis()
        for (Operation op : ops) {
            Operation.Error result = blockUntilComplete(op, timeoutMs - (System.currentTimeMillis() - start))
            if (result != null) return result
        }
        null
    }

    Operation.Error blockUntilComplete(Operation operation, long timeoutMs) throws InterruptedException, IOException {
        def start = System.currentTimeMillis()
        def pollInterval = 5 * 1000
        def opZone = operation.getZone()  // null for global/regional operations

        if (opZone != null) {
            opZone = opZone.split("/").last()
        }
        def status = operation.getStatus()
        def opId = operation.getName()
        while (operation != null && status != "DONE") {
            Thread.sleep(pollInterval)
            long elapsed = System.currentTimeMillis() - start
            if (elapsed >= timeoutMs) {
                throw new InterruptedException("Timed out waiting for operation to complete")
            }
            if (opZone != null) {
                Compute.ZoneOperations.Get get = compute.zoneOperations().get(project, opZone, opId)
                operation = get.execute()
            } else {
                Compute.GlobalOperations.Get get = compute.globalOperations().get(project, opId)
                operation = get.execute()
            }
            status = operation?.getStatus()
        }
        operation?.getError()
    }

    Image lookupImage(String imagePath) throws IOException {
        compute.images().get(project, imageName(imagePath)).execute()
    }

    MachineType lookupMachineType(String machineType) throws IOException {
        compute.machineTypes().get(project, zone, machineType).execute()
    }

    String instanceIdToPrivateDNS(String instanceId) {
        "${instanceId}.c.${project}.internal"
    }

    String publicIpToDns(String ip) {
        if (ip == null) return null
        String[] parts = ip.split("\\.")
        if (parts.length != 4) throw new IllegalArgumentException("Expected IPv4 Public IP address instead of '" + ip + "'")

        // TODO: Is this domain name stable ?
        parts.reverse().join(".")+".bc.googleusercontent.com"
    }

    /**
     * Check if value is valid as a label value as specified here: https://cloud.google.com/compute/docs/labeling-resources
     * @return null if valid or error message
     */
    String validateLabelValue(String value) {
        if (value == null) return null

        if (!value.matches("[a-z0-9-_]*")) {
            return "Value must consist of lowercase letters, numbers, underscores and dashes only"
        }
        if (value.length() > 63) {
            return "Value exceeds maximum length of 63"
        }
        null
    }


    def setStartupScript(Instance instance, String script) {
        addMetadataItem(instance, "startup-script", script)
    }

    def addMetadataItem(Instance instance, String key, String value) {
        def metadata = instance.getMetadata() ?: new Metadata()
        List<Metadata.Items> items = metadata.getItems() ?: []

        Metadata.Items item = new Metadata.Items()
        item.setKey(key)
        item.setValue(value)
        items.add(item)
        metadata.setItems(items)
        instance.setMetadata(metadata)
    }

    String readGoogleMetadata(String meta) {
        try {
            "http://metadata/computeMetadata/v1/$meta".toURL().getText(requestProperties: ['Metadata-Flavor': 'Google'])
        } catch (Exception e) {
            throw new AbortOperationException("Cannot read Google metadata $meta: ${e.getClass()}(${e.getMessage()})",e)
        }
    }

    String readProject() {
        readGoogleMetadata('project/project-id')
    }

    String readZone() {
        readGoogleMetadata('instance/zone').split("/").last()
    }

    String readInstanceId() {
        readGoogleMetadata('instance/name')
    }
}
