Data-Validation

To run this first copy the config/data-validation.properties from /bnsf/hd/config/data-validation/config/data-validation.properties to project root folder

Build it with gradlew build -Pvrsn="VRSN-NUMBER"

Copy build/libs/data-validation-VRSN-NUMBER.jar to lib/data-validation.jar

Run the script as below for Unit testing

./scripts/runTestCases <ARGS as required by script>

Run the script as below for E2E testing - this is essential as Control-M script runWILDPipelines.sh will call it with absolute path

<ABSOLUTE>/scripts/runTestCases <ARGS as required by script>
