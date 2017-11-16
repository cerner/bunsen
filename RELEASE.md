# Releasing Bunsen

## Initial Setup

* A valid Sonatype user account is required to release this project. To create an account sign up with [Sonatype](https://issues.sonatype.org/secure/Signup!default.jspa).
* Execute the following commands in your terminal to prepare travis-ci

    ```
    $ cd /path/to/bunsen
    $ gem install travis
    $ travis login
    #  Prompts you to enter github username, password and two-factor authentication if enabled.
    $ travis enable -r <username>/<repository>
    <username>/<repository>: enabled :)
	```
* Install `gpg2` - we will be using this tool to automatically sign off our artifacts
	* install it via brew - `brew install gpg2`. There are other ways to install this tool but doing it via brew can help us all to be in sync with the version of the tool we are using.
	* Follow this [guide](http://central.sonatype.org/pages/working-with-pgp-signatures.html#generating-a-key-pair) to generate your own gpg key and secret.
	* Choose a password to encrypt the public and private keys that were generated in the previous step using gpg2. Execute the below steps to encrypt the keys.

	```
    $ export ENCRYPTION_PASSWORD=<password to encrypt>
    $ openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in ~/.gnupg/secring.gpg -out .travis/secring.gpg.enc
    $ openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in ~/.gnupg/pubring.gpg -out .travis/pubring.gpg.enc
	```
* All the secrets and passwords must be encrypted and passed on to travis as [secured environment variables](https://docs.travis-ci.com/user/environment-variables/#Defining-encrypted-variables-in-.travis.yml).

	```
    $ travis encrypt --add -r cerner/bunsen SONATYPE_USERNAME=<sonatype username>
    $ travis encrypt --add -r cerner/bunsen SONATYPE_PASSWORD=<sonatype password>
    $ travis encrypt --add -r cerner/bunsen ENCRYPTION_PASSWORD=<password to encrypt>
    $ travis encrypt --add -r cerner/bunsen GPG_KEYNAME=<gpg keyname (ex. 1C06698F)>
    $ travis encrypt --add -r cerner/bunsen GPG_PASSPHRASE=<gpg passphrase>
    ```
* Create a new set of ssh keys to push the documentation site to `gh-pages` branch. Follow this github [documentation](https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent/#generating-a-new-ssh-key) to create the ssh keys.
	* **Note**: The ssh keys file names has to be `deploy_site_key`.
	* Before generating the keys make sure the current directory is the directory where all your ssh keys are stored. By default this would be `~/.ssh`
* Add the contents of `deploy_site_key.pub` to the [bunsen deploy keys](https://github.com/cerner/bunsen/settings/keys).
	* *pro tip*: You can copy the contents using `pbcopy < path/to/deploy_site_key.pub`
* Encrypt the `deploy_site_key` key and add it to .travis.yml file by executing the below commands.

	```
	$ cd path/to/bunsen
	$ travis encrypt-file ~/.ssh/deploy_site --add
	```
* Commit all the changes to the bunsen repo.

## Release process

After preparing the project for the release follow the below steps

* Update the changelog with the release data for the releasing version.
* Commit the change.
* Clean up the previous release backup/release property files.

    ```
    mvn release:clean
    ```
* Prepare the project for releasing.

    ```
    mvn clean release:prepare
    ```
    * The above command will prompt for the following
        * current release version (should be same as in changelog)
        * next development cycle version. After that it also prompts for the next development version
    * Maven builds the project to make sure everything is good. If the build succeeds then it updates the versions of the project and pushes the following to beadledom git repo
        * a commit for the release
        * a commit for the next development cycle
        * the tag that was cut for the release
    * **Note**:
        * Currently accounts that have the ability to perform releases must have [two factor authentication](https://help.github.com/articles/about-two-factor-authentication/) enabled. Because of this we need to generate a [personal access token](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/) to use in lieu of a password during a release.
        * If at anytime the release need to be stopped. Cancel the maven commands using (ctrl + z) and run the below command

        ```
        mvn release:rollback
        ```

* Travis starts a new build for released tag and pushes the artifact to [sonatype staging repo](https://oss.sonatype.org/#stagingpositories).
* Once the artifacts are pushed to the Sonatype staging repo
    * Scroll down to the latest bunsen repo from the list.
    * click on the release button to push the artifact to maven central.
    * **Note**: It roughly takes about 2 hours for the artifacts to sync with the maven central.
* Builds the documentation site for the released tag and publishes it to `gh-pages`.
* Travis starts another build for the current snapshot and pushes the artifacts to [sonatype snapshots repo](https://oss.sonatype.org/content/repositories/snapshots/com/cerner/beadledom/).

To monitor the build go to [bunsen travis-ci builds](https://travis-ci.org/cerner/bunsen/builds).