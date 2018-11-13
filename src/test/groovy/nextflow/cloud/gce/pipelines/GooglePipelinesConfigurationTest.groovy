package nextflow.cloud.gce.pipelines

import spock.lang.Specification

class GooglePipelinesConfigurationTest extends Specification {

    def 'should construct correctly'() {
        given:
        def name = "testName"
        def testZone = ["testZone1","testZone2"]
        def testRegion = ["region1,region2"]
        def instanceType = "testInstanceType"
        def preemp = true
        def remoteBinDir = null

        when:
        def config1 = new GooglePipelinesConfiguration(name,testZone,testRegion,instanceType,remoteBinDir,preemp)
        def config2 = new GooglePipelinesConfiguration(name,testZone,testRegion,instanceType)

        then:
        with(config1) {
            project == name
            zone == testZone
            region == testRegion
            vmInstanceType == instanceType
            preemptible == preemp
        }

        with(config2) {
            project == name
            zone == testZone
            region == testRegion
            vmInstanceType == instanceType
            !preemptible
        }
    }
}