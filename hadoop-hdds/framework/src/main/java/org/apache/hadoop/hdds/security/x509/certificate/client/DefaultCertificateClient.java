/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.client;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.ssl.KeyStoresFactory;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.hdds.security.x509.keys.SecurityUtil;
import org.apache.hadoop.ozone.OzoneSecurityUtil;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.FAILURE;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.GETCERT;
import static org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient.InitResponse.SUCCESS;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.BOOTSTRAP_ERROR;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CERTIFICATE_ERROR;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CRYPTO_SIGNATURE_VERIFICATION_ERROR;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CRYPTO_SIGN_ERROR;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.RENEW_ERROR;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.ROLLBACK_ERROR;

import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;

/**
 * Default Certificate client implementation. It provides certificate
 * operations that needs to be performed by certificate clients in the Ozone
 * eco-system.
 */
public abstract class DefaultCertificateClient implements CertificateClient {

  private static final String CERT_FILE_EXTENSION = ".crt";
  public static final String CERT_FILE_NAME_FORMAT = "%s" + CERT_FILE_EXTENSION;

  private final Logger logger;
  private final SecurityConfig securityConfig;
  private final KeyCodec keyCodec;
  private PrivateKey privateKey;
  private PublicKey publicKey;
  private CertPath certPath;
  private Map<String, CertPath> certificateMap;
  private Set<X509Certificate> rootCaCertificates;
  private Set<X509Certificate> caCertificates;
  private String certSerialId;
  private String caCertId;
  private String rootCaCertId;
  private String component;
  private List<String> pemEncodedCACerts = null;
  private Lock pemEncodedCACertsLock = new ReentrantLock();
  private KeyStoresFactory serverKeyStoresFactory;
  private KeyStoresFactory clientKeyStoresFactory;

  private ScheduledExecutorService executorService;
  private Consumer<String> certIdSaveCallback;
  private Runnable shutdownCallback;
  private SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient;
  private final Set<CertificateNotification> notificationReceivers;
  private RootCaRotationPoller rootCaRotationPoller;

  protected DefaultCertificateClient(
      SecurityConfig securityConfig,
      SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient,
      Logger log,
      String certSerialId,
      String component,
      Consumer<String> saveCertId,
      Runnable shutdown) {
    Objects.requireNonNull(securityConfig);
    this.securityConfig = securityConfig;
    this.scmSecurityClient = scmSecurityClient;
    keyCodec = new KeyCodec(securityConfig, component);
    this.logger = log;
    this.certificateMap = new ConcurrentHashMap<>();
    this.component = component;
    this.certIdSaveCallback = saveCertId;
    this.shutdownCallback = shutdown;
    this.notificationReceivers = new HashSet<>();
    this.rootCaCertificates = ConcurrentHashMap.newKeySet();
    this.caCertificates = ConcurrentHashMap.newKeySet();

    updateCertSerialId(certSerialId);
  }

  /**
   * Load all certificates from configured location.
   * */
  private synchronized void loadAllCertificates() {
    Path path = securityConfig.getCertificateLocation(component);
    if (!path.toFile().exists() && certSerialId == null) {
      return;
    }
    try (Stream<Path> certFiles = Files.list(path)) {
      certFiles
          .filter(Files::isRegularFile)
          .forEach(this::readCertificateFile);
    } catch (IOException e) {
      getLogger().warn("Certificates could not be loaded.", e);
      return;
    }

    if (shouldStartCertificateRenewerService()) {
      if (securityConfig.isAutoCARotationEnabled()) {
        startRootCaRotationPoller();
      }
      if (certPath != null && executorService == null) {
        startCertificateRenewerService();
      } else {
        if (executorService != null) {
          getLogger().debug("CertificateRenewerService is already started.");
        } else {
          getLogger().warn("Component certificate was not loaded.");
        }
      }
    } else {
      getLogger().info("CertificateRenewerService and root ca rotation " +
          "polling is disabled for {}", component);
    }
  }

  private void startRootCaRotationPoller() {
    if (rootCaRotationPoller == null) {
      rootCaRotationPoller = new RootCaRotationPoller(securityConfig,
          new HashSet<>(rootCaCertificates), scmSecurityClient);
      rootCaRotationPoller.addRootCARotationProcessor(
          this::getRootCaRotationListener);
      rootCaRotationPoller.run();
    } else {
      getLogger().debug("Root CA certificate rotation poller is already " +
          "started.");
    }
  }

  @Override
  public void registerRootCARotationListener(
      Function<List<X509Certificate>, CompletableFuture<Void>> listener) {
    if (securityConfig.isAutoCARotationEnabled()) {
      rootCaRotationPoller.addRootCARotationProcessor(listener);
    }
  }

  private synchronized void readCertificateFile(Path filePath) {
    CertificateCodec codec = new CertificateCodec(securityConfig, component);
    String fileName = filePath.getFileName().toString();

    X509Certificate cert;
    try {
      CertPath allCertificates = codec.getCertPath(fileName);
      cert = firstCertificateFrom(allCertificates);
      String readCertSerialId = cert.getSerialNumber().toString();

      if (readCertSerialId.equals(certSerialId)) {
        this.certPath = allCertificates;
      }
      certificateMap.put(readCertSerialId, allCertificates);
      addCertsToSubCaMapIfNeeded(fileName, allCertificates);
      addCertToRootCaMapIfNeeded(fileName, allCertificates);

      updateCachedData(fileName, CAType.SUBORDINATE, this::updateCachedSubCAId);
      updateCachedData(fileName, CAType.ROOT, this::updateCachedRootCAId);

      getLogger().info("Added certificate {} from file: {}.", cert,
          filePath.toAbsolutePath());
    } catch (java.security.cert.CertificateException
             | IOException | IndexOutOfBoundsException e) {
      getLogger().error("Error reading certificate from file: {}.",
          filePath.toAbsolutePath(), e);
    }
  }

  private void updateCachedData(
      String fileName,
      CAType tryCAType,
      Consumer<String> updateCachedId
  ) throws IOException {
    String caTypePrefix = tryCAType.getFileNamePrefix();

    if (fileName.startsWith(caTypePrefix)) {
      updateCachedId.accept(
          fileName.substring(caTypePrefix.length(),
              fileName.length() - CERT_FILE_EXTENSION.length()
          ));
    }
  }

  private synchronized void updateCachedRootCAId(String s) {
    BigInteger candidateNewId = new BigInteger(s);
    if (rootCaCertId == null
        || new BigInteger(rootCaCertId).compareTo(candidateNewId) < 0) {
      rootCaCertId = s;
    }
  }

  private synchronized void updateCachedSubCAId(String s) {
    BigInteger candidateNewId = new BigInteger(s);
    if (caCertId == null
        || new BigInteger(caCertId).compareTo(candidateNewId) < 0) {
      caCertId = s;
    }
  }

  private void addCertsToSubCaMapIfNeeded(String fileName, CertPath certs) {
    if (fileName.startsWith(CAType.SUBORDINATE.getFileNamePrefix())) {
      caCertificates.addAll(
          certs.getCertificates().stream()
              .map(x -> (X509Certificate) x)
              .collect(Collectors.toSet()));
    }
  }

  private void addCertToRootCaMapIfNeeded(String fileName, CertPath certs) {
    if (fileName.startsWith(CAType.ROOT.getFileNamePrefix())) {
      rootCaCertificates.add(firstCertificateFrom(certs));
    }
  }

  /**
   * Returns the private key of the specified  if it exists on the local
   * system.
   *
   * @return private key or Null if there is no data.
   */
  @Override
  public synchronized PrivateKey getPrivateKey() {
    if (privateKey != null) {
      return privateKey;
    }

    Path keyPath = securityConfig.getKeyLocation(component);
    if (OzoneSecurityUtil.checkIfFileExist(keyPath,
        securityConfig.getPrivateKeyFileName())) {
      try {
        privateKey = keyCodec.readPrivateKey();
      } catch (InvalidKeySpecException | NoSuchAlgorithmException
          | IOException e) {
        getLogger().error("Error while getting private key.", e);
      }
    }
    return privateKey;
  }

  /**
   * Returns the public key of the specified if it exists on the local system.
   *
   * @return public key or Null if there is no data.
   */
  @Override
  public PublicKey getPublicKey() {
    if (publicKey != null) {
      return publicKey;
    }

    Path keyPath = securityConfig.getKeyLocation(component);
    if (OzoneSecurityUtil.checkIfFileExist(keyPath,
        securityConfig.getPublicKeyFileName())) {
      try {
        publicKey = keyCodec.readPublicKey();
      } catch (InvalidKeySpecException | NoSuchAlgorithmException
          | IOException e) {
        getLogger().error("Error while getting public key.", e);
      }
    }
    return publicKey;
  }

  @Override
  public X509Certificate getCertificate() {
    CertPath currentCertPath = getCertPath();
    if (currentCertPath == null || currentCertPath.getCertificates() == null) {
      return null;
    }
    return firstCertificateFrom(currentCertPath);
  }

  /**
   * Returns the default certificate of given client if it exists.
   *
   * @return certificate or Null if there is no data.
   */
  @Override
  public synchronized CertPath getCertPath() {
    if (certPath != null) {
      return certPath;
    }

    if (certSerialId == null) {
      getLogger().error("Default certificate serial id is not set. Can't " +
          "locate the default certificate for this client.");
      return null;
    }
    // Refresh the cache from file system.
    loadAllCertificates();
    if (certificateMap.containsKey(certSerialId)) {
      certPath = certificateMap.get(certSerialId);
    }
    return certPath;
  }

  /**
   * Return the latest CA certificate known to the client.
   *
   * @return latest ca certificate known to the client.
   */
  @Override
  public synchronized X509Certificate getCACertificate() {
    CertPath caCertPath = getCACertPath();
    if (caCertPath == null || caCertPath.getCertificates() == null) {
      return null;
    }
    return firstCertificateFrom(caCertPath);
  }

  /**
   * Return all certificates in this component's trust chain,
   * the last one is the root CA certificate.
   */
  @Override
  public synchronized List<X509Certificate> getTrustChain()
      throws IOException {
    CertPath path = getCertPath();
    if (path == null || path.getCertificates() == null) {
      return null;
    }

    List<X509Certificate> chain = new ArrayList<>();
    // certificate bundle case
    if (path.getCertificates().size() > 1) {
      for (int i = 0; i < path.getCertificates().size(); i++) {
        chain.add((X509Certificate) path.getCertificates().get(i));
      }
    } else {
      // case before certificate bundle is supported
      X509Certificate cert = getCertificate();
      if (cert != null) {
        chain.add(getCertificate());
      }
      cert = getCACertificate();
      if (cert != null) {
        chain.add(getCACertificate());
      }
      cert = getRootCACertificate();
      if (cert != null) {
        chain.add(cert);
      }
      Preconditions.checkState(chain.size() > 0, "Empty trust chain");
    }
    return chain;
  }

  public synchronized CertPath getCACertPath() {
    if (caCertId != null) {
      return certificateMap.get(caCertId);
    }
    return null;
  }

  /**
   * Returns the certificate  with the specified certificate serial id if it
   * exists else try to get it from SCM.
   *
   * @param certId
   * @return certificate or Null if there is no data.
   */
  @Override
  public synchronized X509Certificate getCertificate(String certId)
      throws CertificateException {
    // Check if it is in cache.
    if (certificateMap.containsKey(certId) &&
        certificateMap.get(certId).getCertificates() != null) {
      return firstCertificateFrom(certificateMap.get(certId));
    }
    // Try to get it from SCM.
    return this.getCertificateFromScm(certId);
  }

  /**
   * Get certificate from SCM and store it in local file system.
   * @param certId
   * @return certificate
   */
  private X509Certificate getCertificateFromScm(String certId)
      throws CertificateException {

    getLogger().info("Getting certificate with certSerialId:{}.",
        certId);
    try {
      String pemEncodedCert = getScmSecureClient().getCertificate(certId);
      this.storeCertificate(pemEncodedCert, CAType.NONE);
      return CertificateCodec.getX509Certificate(pemEncodedCert);
    } catch (Exception e) {
      getLogger().error("Error while getting Certificate with " +
          "certSerialId:{} from scm.", certId, e);
      throw new CertificateException("Error while getting certificate for " +
          "certSerialId:" + certId, e, CERTIFICATE_ERROR);
    }
  }

  /**
   * Creates digital signature over the data stream using the s private key.
   *
   * @param data - Data to sign.
   * @throws CertificateException - on Error.
   */
  @Override
  public byte[] signData(byte[] data) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(securityConfig.getSignatureAlgo(),
          securityConfig.getProvider());

      sign.initSign(getPrivateKey());
      sign.update(data);

      return sign.sign();
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGN_ERROR);
    }
  }

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   *
   * @param data - Data in byte array.
   * @param signature - Byte Array containing the signature.
   * @param cert - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  @Override
  public boolean verifySignature(byte[] data, byte[] signature,
      X509Certificate cert) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(securityConfig.getSignatureAlgo(),
          securityConfig.getProvider());
      sign.initVerify(cert);
      sign.update(data);
      return sign.verify(signature);
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGNATURE_VERIFICATION_ERROR);
    }
  }

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   *
   * @param data - Data in byte array.
   * @param signature - Byte Array containing the signature.
   * @param pubKey - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  private boolean verifySignature(byte[] data, byte[] signature,
      PublicKey pubKey) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(securityConfig.getSignatureAlgo(),
          securityConfig.getProvider());
      sign.initVerify(pubKey);
      sign.update(data);
      return sign.verify(signature);
    } catch (NoSuchAlgorithmException | NoSuchProviderException
        | InvalidKeyException | SignatureException e) {
      getLogger().error("Error while signing the stream", e);
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGNATURE_VERIFICATION_ERROR);
    }
  }

  /**
   * Returns a CSR builder that can be used to creates a Certificate signing
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  @Override
  public CertificateSignRequest.Builder getCSRBuilder()
      throws CertificateException {
    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setConfiguration(securityConfig)
            .addInetAddresses();
    return builder;
  }

  /**
   * Stores the Certificate  for this client. Don't use this api to add trusted
   * certificates of others.
   *
   * @param pemEncodedCert - pem encoded X509 Certificate
   * @param caType         - Is CA certificate.
   * @throws CertificateException - on Error.
   */
  @Override
  public void storeCertificate(String pemEncodedCert,
      CAType caType) throws CertificateException {
    CertificateCodec certificateCodec = new CertificateCodec(securityConfig,
        component);
    storeCertificate(pemEncodedCert, caType,
        certificateCodec, true, false);
  }

  public synchronized void storeCertificate(String pemEncodedCert,
      CAType caType, CertificateCodec codec, boolean addToCertMap,
      boolean updateCA) throws CertificateException {
    try {
      CertPath certificatePath =
          CertificateCodec.getCertPathFromPemEncodedString(pemEncodedCert);
      X509Certificate cert = firstCertificateFrom(certificatePath);

      String certName = String.format(CERT_FILE_NAME_FORMAT,
          caType.getFileNamePrefix() + cert.getSerialNumber().toString());

      if (updateCA) {
        if (caType == CAType.SUBORDINATE) {
          caCertId = cert.getSerialNumber().toString();
        }
        if (caType == CAType.ROOT) {
          rootCaCertId = cert.getSerialNumber().toString();
        }
      }

      codec.writeCertificate(certName,
          pemEncodedCert);
      if (addToCertMap) {
        certificateMap.put(cert.getSerialNumber().toString(), certificatePath);
      }
    } catch (IOException | java.security.cert.CertificateException e) {
      throw new CertificateException("Error while storing certificate.", e,
          CERTIFICATE_ERROR);
    }
  }

  /**
   * Defines 8 cases of initialization.
   * Each case specifies objects found.
   * 0. NONE                  Keypair as well as certificate not found.
   * 1. CERT                  Certificate found but keypair missing.
   * 2. PUBLIC_KEY            Public key found but private key and
   *                          certificate is missing.
   * 3. PUBLICKEY_CERT        Only public key and certificate is present.
   * 4. PRIVATE_KEY           Only private key is present.
   * 5. PRIVATEKEY_CERT       Only private key and certificate is present.
   * 6. PUBLICKEY_PRIVATEKEY  indicates private and public key were read
   *                          successfully from configured location but
   *                          Certificate.
   * 7. ALL                   Keypair as well as certificate is present.
   * */
  protected enum InitCase {
    NONE,
    CERT,
    PUBLIC_KEY,
    PUBLICKEY_CERT,
    PRIVATE_KEY,
    PRIVATEKEY_CERT,
    PUBLICKEY_PRIVATEKEY,
    ALL
  }

  /**
   *
   * Initializes client by performing following actions.
   * 1. Create key dir if not created already.
   * 2. Generates and stores a keypair.
   * 3. Try to recover public key if private key and certificate is present
   *    but public key is missing.
   *
   * Truth table:
   *  +--------------+-----------------+--------------+----------------+
   *  | Private Key  | Public Keys     | Certificate  |   Result       |
   *  +--------------+-----------------+--------------+----------------+
   *  | False  (0)   | False   (0)     | False  (0)   |   GETCERT  000 |
   *  | False  (0)   | False   (0)     | True   (1)   |   FAILURE  001 |
   *  | False  (0)   | True    (1)     | False  (0)   |   FAILURE  010 |
   *  | False  (0)   | True    (1)     | True   (1)   |   FAILURE  011 |
   *  | True   (1)   | False   (0)     | False  (0)   |   FAILURE  100 |
   *  | True   (1)   | False   (0)     | True   (1)   |   SUCCESS  101 |
   *  | True   (1)   | True    (1)     | False  (0)   |   GETCERT  110 |
   *  | True   (1)   | True    (1)     | True   (1)   |   SUCCESS  111 |
   *  +--------------+-----------------+--------------+----------------+
   *
   * @return InitResponse
   * Returns FAILURE in following cases:
   * 1. If private key is missing but public key or certificate is available.
   * 2. If public key and certificate is missing.
   *
   * Returns SUCCESS in following cases:
   * 1. If keypair as well certificate is available.
   * 2. If private key and certificate is available and public key is
   *    recovered successfully.
   *
   * Returns GETCERT in following cases:
   * 1. First time when keypair and certificate is not available, keypair
   *    will be generated and stored at configured location.
   * 2. When keypair (public/private key) is available but certificate is
   *    missing.
   */
  @Override
  public synchronized InitResponse init() throws CertificateException {
    int initCase = 0;
    PrivateKey pvtKey = getPrivateKey();
    PublicKey pubKey = getPublicKey();
    X509Certificate certificate = getCertificate();
    if (pvtKey != null) {
      initCase = initCase | 1 << 2;
    }
    if (pubKey != null) {
      initCase = initCase | 1 << 1;
    }
    if (certificate != null) {
      initCase = initCase | 1;
    }

    getLogger().info("Certificate client init case: {}", initCase);
    Preconditions.checkArgument(initCase < InitCase.values().length, "Not a " +
        "valid case.");
    InitCase init = InitCase.values()[initCase];
    return handleCase(init);
  }

  private X509Certificate firstCertificateFrom(CertPath certificatePath) {
    return CertificateCodec.firstCertificateFrom(certificatePath);
  }

  /**
   * Default handling of each {@link InitCase}.
   */
  protected InitResponse handleCase(InitCase init)
      throws CertificateException {
    switch (init) {
    case NONE:
      getLogger().info("Creating keypair for client as keypair and " +
          "certificate not found.");
      bootstrapClientKeys();
      return GETCERT;
    case CERT:
      getLogger().error("Private key not found, while certificate is still" +
          " present. Delete keypair and try again.");
      return FAILURE;
    case PUBLIC_KEY:
      getLogger().error("Found public key but private key and certificate " +
          "missing.");
      return FAILURE;
    case PRIVATE_KEY:
      getLogger().info("Found private key but public key and certificate " +
          "is missing.");
      // TODO: Recovering public key from private might be possible in some
      //  cases.
      return FAILURE;
    case PUBLICKEY_CERT:
      getLogger().error("Found public key and certificate but private " +
          "key is missing.");
      return FAILURE;
    case PRIVATEKEY_CERT:
      getLogger().info("Found private key and certificate but public key" +
          " missing.");
      if (recoverPublicKey()) {
        return SUCCESS;
      } else {
        getLogger().error("Public key recovery failed.");
        return FAILURE;
      }
    case PUBLICKEY_PRIVATEKEY:
      getLogger().info("Found private and public key but certificate is" +
          " missing.");
      if (validateKeyPair(getPublicKey())) {
        return GETCERT;
      } else {
        getLogger().info("Keypair validation failed.");
        return FAILURE;
      }
    case ALL:
      getLogger().info("Found certificate file along with KeyPair.");
      if (validateKeyPairAndCertificate()) {
        return SUCCESS;
      } else {
        return FAILURE;
      }
    default:
      getLogger().error("Unexpected case: {} (private/public/cert)",
          Integer.toBinaryString(init.ordinal()));

      return FAILURE;
    }
  }

  /**
   * Validate keypair and certificate.
   * */
  protected boolean validateKeyPairAndCertificate() throws
      CertificateException {
    if (validateKeyPair(getPublicKey())) {
      getLogger().info("Keypair validated.");
      // TODO: Certificates cryptographic validity can be checked as well.
      if (validateKeyPair(getCertificate().getPublicKey())) {
        getLogger().info("Keypair validated with certificate.");
      } else {
        getLogger().error("Stored certificate is generated with different " +
            "private key.");
        return false;
      }
    } else {
      getLogger().error("Keypair validation failed.");
      return false;
    }
    return true;
  }

  /**
   * Tries to recover public key from certificate. Also validates recovered
   * public key.
   * */
  protected boolean recoverPublicKey() throws CertificateException {
    PublicKey pubKey = getCertificate().getPublicKey();
    try {

      if (validateKeyPair(pubKey)) {
        keyCodec.writePublicKey(pubKey);
        publicKey = pubKey;
      } else {
        getLogger().error("Can't recover public key " +
            "corresponding to private key.");
        return false;
      }
    } catch (IOException e) {
      throw new CertificateException("Error while trying to recover " +
          "public key.", e, BOOTSTRAP_ERROR);
    }
    return true;
  }

  /**
   * Validates public and private key of certificate client.
   *
   * @param pubKey
   * */
  protected boolean validateKeyPair(PublicKey pubKey)
      throws CertificateException {
    byte[] challenge =
        RandomStringUtils.random(1000, 0, 0, false, false, null,
                new SecureRandom()).getBytes(StandardCharsets.UTF_8);
    return verifySignature(challenge, signData(challenge), pubKey);
  }

  /**
   * Bootstrap the client by creating keypair and storing it in configured
   * location.
   * */
  protected void bootstrapClientKeys() throws CertificateException {
    Path keyPath = securityConfig.getKeyLocation(component);
    if (Files.notExists(keyPath)) {
      try {
        Files.createDirectories(keyPath);
      } catch (IOException e) {
        throw new CertificateException("Error while creating directories " +
            "for certificate storage.", BOOTSTRAP_ERROR);
      }
    }
    KeyPair keyPair = createKeyPair(keyCodec);
    privateKey = keyPair.getPrivate();
    publicKey = keyPair.getPublic();
  }

  protected KeyPair createKeyPair(KeyCodec codec) throws CertificateException {
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = null;
    try {
      keyPair = keyGenerator.generateKey();
      codec.writePublicKey(keyPair.getPublic());
      codec.writePrivateKey(keyPair.getPrivate());
    } catch (NoSuchProviderException | NoSuchAlgorithmException
        | IOException e) {
      getLogger().error("Error while bootstrapping certificate client.", e);
      throw new CertificateException("Error while bootstrapping certificate.",
          BOOTSTRAP_ERROR);
    }
    return keyPair;
  }

  public Logger getLogger() {
    return logger;
  }

  public String getComponentName() {
    return component;
  }

  @Override
  public synchronized X509Certificate getRootCACertificate() {
    if (rootCaCertId != null) {
      return firstCertificateFrom(certificateMap.get(rootCaCertId));
    }
    return null;
  }

  @Override
  public Set<X509Certificate> getAllRootCaCerts() {
    Set<X509Certificate> certs =
        Collections.unmodifiableSet(rootCaCertificates);
    getLogger().info("{} has {} Root CA certificates", this.component,
        certs.size());
    return certs;
  }

  @Override
  public Set<X509Certificate> getAllCaCerts() {
    Set<X509Certificate> certs = Collections.unmodifiableSet(caCertificates);
    getLogger().info("{} has {} CA certificates", this.component,
        certs.size());
    return certs;
  }

  @Override
  public List<String> getCAList() {
    pemEncodedCACertsLock.lock();
    try {
      return pemEncodedCACerts;
    } finally {
      pemEncodedCACertsLock.unlock();
    }
  }

  @Override
  public List<String> listCA() throws IOException {
    pemEncodedCACertsLock.lock();
    try {
      if (pemEncodedCACerts == null) {
        updateCAList();
      }
      return pemEncodedCACerts;
    } finally {
      pemEncodedCACertsLock.unlock();
    }
  }

  @Override
  public List<String> updateCAList() throws IOException {
    pemEncodedCACertsLock.lock();
    try {
      pemEncodedCACerts = getScmSecureClient().listCACertificate();
      return pemEncodedCACerts;
    } catch (Exception e) {
      getLogger().error("Error during updating CA list", e);
      throw new CertificateException("Error during updating CA list", e,
          CERTIFICATE_ERROR);
    } finally {
      pemEncodedCACertsLock.unlock();
    }
  }

  @Override
  public synchronized KeyStoresFactory getServerKeyStoresFactory()
      throws CertificateException {
    if (serverKeyStoresFactory == null) {
      serverKeyStoresFactory = SecurityUtil.getServerKeyStoresFactory(
          securityConfig, this, true);
    }
    return serverKeyStoresFactory;
  }

  @Override
  public KeyStoresFactory getClientKeyStoresFactory()
      throws CertificateException {
    if (clientKeyStoresFactory == null) {
      clientKeyStoresFactory = SecurityUtil.getClientKeyStoresFactory(
          securityConfig, this, true);
    }
    return clientKeyStoresFactory;
  }

  /**
   * Register a receiver that will be called after the certificate renewed.
   * @param receiver
   */
  @Override
  public void registerNotificationReceiver(CertificateNotification receiver) {
    synchronized (notificationReceivers) {
      notificationReceivers.add(receiver);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }

    if (rootCaRotationPoller != null) {
      rootCaRotationPoller.close();
    }

    if (serverKeyStoresFactory != null) {
      serverKeyStoresFactory.destroy();
    }

    if (clientKeyStoresFactory != null) {
      clientKeyStoresFactory.destroy();
    }
  }

  /**
   * Check how much time before certificate will enter expiry grace period.
   * @return Duration, time before certificate enters the grace
   *                   period defined by "hdds.x509.renew.grace.duration"
   */
  public Duration timeBeforeExpiryGracePeriod(X509Certificate certificate) {
    Duration gracePeriod = securityConfig.getRenewalGracePeriod();
    Date expireDate = certificate.getNotAfter();
    LocalDateTime gracePeriodStart = expireDate.toInstant()
        .minus(gracePeriod).atZone(ZoneId.systemDefault()).toLocalDateTime();
    LocalDateTime currentTime = LocalDateTime.now();
    if (gracePeriodStart.isBefore(currentTime)) {
      // Cert is already in grace period time.
      return Duration.ZERO;
    } else {
      return Duration.between(currentTime, gracePeriodStart);
    }
  }

  /**
   * Renew keys and certificate. Save the keys are certificate to disk in new
   * directories, swap the current key directory and certs directory with the
   * new directories.
   * @param force, check certificate expiry time again if force is false.
   * @return String, new certificate ID
   * */
  public String renewAndStoreKeyAndCertificate(boolean force)
      throws CertificateException {
    if (!force) {
      synchronized (this) {
        Preconditions.checkArgument(
            timeBeforeExpiryGracePeriod(firstCertificateFrom(certPath))
                .isZero());
      }
    }

    String newKeyPath = securityConfig.getKeyLocation(component)
        .toString() + HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX;
    String newCertPath = securityConfig.getCertificateLocation(component)
        .toString() + HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX;
    File newKeyDir = new File(newKeyPath);
    File newCertDir = new File(newCertPath);
    try {
      FileUtils.deleteDirectory(newKeyDir);
      FileUtils.deleteDirectory(newCertDir);
      Files.createDirectories(newKeyDir.toPath());
      Files.createDirectories(newCertDir.toPath());
    } catch (IOException e) {
      throw new CertificateException("Error while deleting/creating " +
          newKeyPath + " or " + newCertPath + " directories to cleanup " +
          " certificate storage. ", e, RENEW_ERROR);
    }

    // Generate key
    KeyCodec newKeyCodec = new KeyCodec(securityConfig, newKeyDir.toPath());
    KeyPair newKeyPair;
    try {
      newKeyPair = createKeyPair(newKeyCodec);
    } catch (CertificateException e) {
      throw new CertificateException("Error while creating new key pair.",
          e, RENEW_ERROR);
    }

    // Get certificate signed
    String newCertSerialId;
    try {
      CertificateSignRequest.Builder csrBuilder = getCSRBuilder();
      csrBuilder.setKey(newKeyPair);
      newCertSerialId = signAndStoreCertificate(csrBuilder.build(),
          Paths.get(newCertPath), true);
    } catch (Exception e) {
      throw new CertificateException("Error while signing and storing new" +
          " certificates.", e, RENEW_ERROR);
    }

    // switch Key and Certs directory on disk
    File currentKeyDir = new File(
        securityConfig.getKeyLocation(component).toString());
    File currentCertDir = new File(
        securityConfig.getCertificateLocation(component).toString());
    File backupKeyDir = new File(
        securityConfig.getKeyLocation(component).toString() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);
    File backupCertDir = new File(
        securityConfig.getCertificateLocation(component).toString() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);

    try {
      Files.move(currentKeyDir.toPath(), backupKeyDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      // Cannot move current key dir to the backup dir
      throw new CertificateException("Failed to move " +
          currentKeyDir.getAbsolutePath() +
          " to " + backupKeyDir.getAbsolutePath() + " during " +
          "certificate renew.", RENEW_ERROR);
    }

    try {
      Files.move(currentCertDir.toPath(), backupCertDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      // Cannot move current cert dir to the backup dir
      rollbackBackupDir(currentKeyDir, currentCertDir, backupKeyDir,
          backupCertDir);
      throw new CertificateException("Failed to move " +
          currentCertDir.getAbsolutePath() +
          " to " + backupCertDir.getAbsolutePath() + " during " +
          "certificate renew.", RENEW_ERROR);
    }

    try {
      Files.move(newKeyDir.toPath(), currentKeyDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      // Cannot move new dir as the current dir
      String msg = "Failed to move " + newKeyDir.getAbsolutePath() +
          " to " + currentKeyDir.getAbsolutePath() +
          " during certificate renew.";
      // rollback
      rollbackBackupDir(currentKeyDir, currentCertDir, backupKeyDir,
          backupCertDir);
      throw new CertificateException(msg, RENEW_ERROR);
    }

    try {
      Files.move(newCertDir.toPath(), currentCertDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      // Cannot move new dir as the current dir
      String msg = "Failed to move " + newCertDir.getAbsolutePath() +
          " to " + currentCertDir.getAbsolutePath() +
          " during certificate renew.";
      // delete currentKeyDir which is moved from new key directory
      try {
        FileUtils.deleteDirectory(new File(currentKeyDir.toString()));
      } catch (IOException e1) {
        getLogger().error("Failed to delete current KeyDir {} which is moved " +
            " from the newly generated KeyDir {}", currentKeyDir, newKeyDir, e);
        throw new CertificateException(msg, RENEW_ERROR);
      }
      // rollback
      rollbackBackupDir(currentKeyDir, currentCertDir, backupKeyDir,
          backupCertDir);
      throw new CertificateException(msg, RENEW_ERROR);
    }

    getLogger().info("Successful renew key and certificate." +
        " New certificate {}.", newCertSerialId);
    return newCertSerialId;
  }

  private void rollbackBackupDir(File currentKeyDir, File currentCertDir,
      File backupKeyDir, File backupCertDir) throws CertificateException {
    // move backup dir back as current dir
    try {
      Files.move(backupKeyDir.toPath(), currentKeyDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      String msg = "Failed to move " + backupKeyDir.getAbsolutePath() +
          " back to " + currentKeyDir.getAbsolutePath() +
          " during rollback.";
      // Need a manual recover process.
      throw new CertificateException(msg, ROLLBACK_ERROR);
    }

    try {
      Files.move(backupCertDir.toPath(), currentCertDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      String msg = "Failed to move " + backupCertDir.getAbsolutePath() +
          " back to " + currentCertDir.getAbsolutePath()  +
          " during rollback.";
      // Need a manual recover process.
      throw new CertificateException(msg, ROLLBACK_ERROR);
    }

    Preconditions.checkArgument(currentCertDir.exists());
    Preconditions.checkArgument(currentKeyDir.exists());
  }

  /**
   * Delete old backup key and cert directory.
   */
  public void cleanBackupDir() {
    File backupKeyDir = new File(
        securityConfig.getKeyLocation(component).toString() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);
    File backupCertDir = new File(
        securityConfig.getCertificateLocation(component).toString() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX);
    if (backupKeyDir.exists()) {
      try {
        FileUtils.deleteDirectory(backupKeyDir);
      } catch (IOException e) {
        getLogger().error("Error while deleting {} directories for " +
            "certificate storage cleanup.", backupKeyDir, e);
      }
    }
    if (backupCertDir.exists()) {
      try {
        FileUtils.deleteDirectory(backupCertDir);
      } catch (IOException e) {
        getLogger().error("Error while deleting {} directories for " +
            "certificate storage cleanup.", backupCertDir, e);
      }
    }
  }

  public synchronized void reloadKeyAndCertificate(String newCertId) {
    privateKey = null;
    publicKey = null;
    certPath = null;
    caCertId = null;
    rootCaCertId = null;

    String oldCaCertId = updateCertSerialId(newCertId);
    getLogger().info("Reset and reloaded key and all certificates for new " +
        "certificate {}.", newCertId);

    // notify notification receivers
    notificationReceivers.forEach(r -> r.notifyCertificateRenewed(
        this, oldCaCertId, newCertId));
  }

  public SecurityConfig getSecurityConfig() {
    return securityConfig;
  }

  private synchronized String updateCertSerialId(String newCertSerialId) {
    certSerialId = newCertSerialId;
    getLogger().info("Certificate serial ID set to {}", certSerialId);
    loadAllCertificates();
    return certSerialId;
  }

  protected String signAndStoreCertificate(
      PKCS10CertificationRequest request, Path certificatePath)
      throws CertificateException {
    return signAndStoreCertificate(request, certificatePath, false);
  }

  protected abstract SCMGetCertResponseProto getCertificateSignResponse(
      PKCS10CertificationRequest request) throws IOException;

  protected String signAndStoreCertificate(
      PKCS10CertificationRequest request, Path certificatePath, boolean renew)
      throws CertificateException {
    try {
      SCMGetCertResponseProto response = getCertificateSignResponse(request);

      // Persist certificates.
      if (response.hasX509CACertificate()) {
        String pemEncodedCert = response.getX509Certificate();
        CertificateCodec certCodec = new CertificateCodec(
            getSecurityConfig(), certificatePath);
        // Certs will be added to cert map after reloadAllCertificate called
        storeCertificate(pemEncodedCert, CAType.NONE,
            certCodec, false, !renew);
        storeCertificate(response.getX509CACertificate(),
            CAType.SUBORDINATE, certCodec, false, !renew);

        getAndStoreAllRootCAs(certCodec, renew);
        // Return the default certificate ID
        return updateCertSerialId(CertificateCodec
            .getX509Certificate(pemEncodedCert).getSerialNumber().toString());
      } else {
        throw new CertificateException("Unable to retrieve " +
            "certificate chain.");
      }
    } catch (IOException | java.security.cert.CertificateException e) {
      logger.error("Error while signing and storing SCM signed certificate.",
          e);
      throw new CertificateException(
          "Error while signing and storing SCM signed certificate.", e);
    }
  }

  private void getAndStoreAllRootCAs(CertificateCodec certCodec, boolean renew)
      throws IOException {
    List<String> rootCAPems = scmSecurityClient.getAllRootCaCertificates();
    for (String rootCAPem : rootCAPems) {
      storeCertificate(rootCAPem, CAType.ROOT, certCodec,
          false, !renew);
    }
  }

  public String signAndStoreCertificate(
      PKCS10CertificationRequest request) throws CertificateException {
    return updateCertSerialId(signAndStoreCertificate(request,
        securityConfig.getCertificateLocation(getComponentName())));
  }

  public SCMSecurityProtocolClientSideTranslatorPB getScmSecureClient()
      throws IOException {
    return scmSecurityClient;
  }

  protected boolean shouldStartCertificateRenewerService() {
    return true;
  }

  public synchronized CompletableFuture<Void> getRootCaRotationListener(
      List<X509Certificate> rootCAs) {
    if (rootCaCertificates.containsAll(rootCAs)) {
      return CompletableFuture.completedFuture(null);
    }
    CertificateRenewerService renewerService =
        new CertificateRenewerService(true);
    return CompletableFuture.runAsync(renewerService, executorService);
  }

  public synchronized void startCertificateRenewerService() {
    Preconditions.checkNotNull(getCertificate(),
        "Component certificate should not be empty");
    // Schedule task to refresh certificate before it expires
    Duration gracePeriod = securityConfig.getRenewalGracePeriod();
    long timeBeforeGracePeriod =
        timeBeforeExpiryGracePeriod(firstCertificateFrom(certPath)).toMillis();
    // At least three chances to renew the certificate before it expires
    long interval =
        Math.min(gracePeriod.toMillis() / 3, TimeUnit.DAYS.toMillis(1));

    if (executorService == null) {
      executorService = Executors.newScheduledThreadPool(1,
          new ThreadFactoryBuilder().setNameFormat(
                  getComponentName() + "-CertificateRenewerService")
              .setDaemon(true).build());
    }
    this.executorService.scheduleAtFixedRate(
        new CertificateRenewerService(false),
        timeBeforeGracePeriod, interval, TimeUnit.MILLISECONDS);
    getLogger().info("CertificateRenewerService for {} is started with " +
            "first delay {} ms and interval {} ms.", component,
        timeBeforeGracePeriod, interval);
  }

  /**
   * Task to monitor certificate lifetime and renew the certificate if needed.
   */
  public class CertificateRenewerService implements Runnable {
    private boolean forceRenewal;

    public CertificateRenewerService(boolean forceRenewal) {
      this.forceRenewal = forceRenewal;
    }

    @Override
    public void run() {
      // Lock to protect the certificate renew process, to make sure there is
      // only one renew process is ongoing at one time.
      // Certificate renew steps:
      //  1. generate new keys and sign new certificate, persist data to disk
      //  2. switch on disk new keys and certificate with current ones
      //  3. save new certificate ID into service VERSION file
      //  4. refresh in memory certificate ID and reload all new certificates
      synchronized (DefaultCertificateClient.class) {
        X509Certificate currentCert = getCertificate();
        Duration timeLeft = timeBeforeExpiryGracePeriod(currentCert);

        if (!forceRenewal && !timeLeft.isZero()) {
          return;
        }
        String newCertId;
        try {
          getLogger().info("Current certificate {} needs to be renewed " +
                  "remaining grace period {}. Forced renewal due to root ca " +
                  "rotation: {}.",
              currentCert.getSerialNumber().toString(),
              timeLeft, forceRenewal);
          newCertId = renewAndStoreKeyAndCertificate(forceRenewal);
        } catch (CertificateException e) {
          if (e.errorCode() ==
              CertificateException.ErrorCode.ROLLBACK_ERROR) {
            if (shutdownCallback != null) {
              getLogger().error("Failed to rollback key and cert after an " +
                  " unsuccessful renew try.", e);
              shutdownCallback.run();
            }
          }
          getLogger().error("Failed to renew and store key and cert." +
              " Keep using existing certificates.", e);
          return;
        }

        // Persist new cert serial id in component VERSION file
        if (certIdSaveCallback != null) {
          certIdSaveCallback.accept(newCertId);
        }

        // reset and reload all certs
        reloadKeyAndCertificate(newCertId);
        // cleanup backup directory
        cleanBackupDir();
      }
    }
  }

  /**
   * Set the CA certificate. For TEST only.
   */
  @VisibleForTesting
  public synchronized void setCACertificate(X509Certificate cert)
      throws Exception {
    caCertId = cert.getSerialNumber().toString();
    certificateMap.put(caCertId,
        CertificateCodec.getCertPathFromPemEncodedString(
            CertificateCodec.getPEMEncodedString(cert)));
  }
}
