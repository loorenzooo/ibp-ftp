/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.ibp.nifi.processors;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPHTTPClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.FileInfo;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.SocksProxySocketFactory;

public class IBPFTPTransfer extends FTPTransfer {

	private ComponentLog logger = null;

	private final ProcessContext ctx;
	private boolean closed = true;
	private FTPClient client;
	private String homeDirectory;
	private String remoteHostName;

	// Processor properties
	public static final PropertyDescriptor MAX_DEPTH = new PropertyDescriptor.Builder().name("MaximumDepth")
			.description("The maximum depth for fetching files")
			.addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).required(false).build();

    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
            .name("Search Recursively")
            .description("If true, will pull files from arbitrarily nested subdirectories; otherwise, will not traverse subdirectories")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    
    public static final PropertyDescriptor PATH_FILTER_REGEX = new PropertyDescriptor.Builder()
            .name("Path Filter Regex")
            .description("When " + RECURSIVE_SEARCH.getName() + " is true, then only subdirectories whose path matches the given Regular Expression will be scanned")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor FILE_FILTER_REGEX = new PropertyDescriptor.Builder()
            .name("File Filter Regex")
            .description("Provides a Java Regular Expression for filtering Filenames; if a filter is supplied, only files whose names match that Regular Expression will be fetched")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();
    
    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The fully qualified hostname or IP address of the remote system")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(true)
            .build();
    
	public IBPFTPTransfer(final ProcessContext context, final ProcessorLog logger) {
		super(context, logger);
		this.ctx = context;
		this.logger = logger;
	}

	@Override
	public List<FileInfo> getListing() throws IOException {
		final String path = ctx.getProperty(FileTransfer.REMOTE_PATH).evaluateAttributeExpressions().getValue();
		final int depth = 0;
		final int maxResults = ctx.getProperty(FileTransfer.REMOTE_POLL_BATCH_SIZE).asInteger();
		final Integer maxDepth = ctx.getProperty(MAX_DEPTH).asInteger();
		return getListing(path, depth, maxResults, maxDepth);
	}

	private List<FileInfo> getListing(final String path, final int depth, final int maxResults, final Integer maxDepth)
			throws IOException {
		
		final List<FileInfo> listing = new ArrayList<>();
		if (maxResults < 1) {
			return listing;
		}

		if (depth >= 100) {
			logger.warn(this + " had to stop recursively searching directories at a recursive depth of " + depth
					+ " to avoid memory issues");
			return listing;
		}

		final boolean ignoreDottedFiles = ctx.getProperty(FileTransfer.IGNORE_DOTTED_FILES).asBoolean();
		final boolean recurse = ctx.getProperty(FileTransfer.RECURSIVE_SEARCH).asBoolean();
		final String fileFilterRegex = ctx.getProperty(FileTransfer.FILE_FILTER_REGEX).getValue();
		final Pattern pattern = (fileFilterRegex == null) ? null : Pattern.compile(fileFilterRegex);
		final String pathFilterRegex = ctx.getProperty(FileTransfer.PATH_FILTER_REGEX).getValue();
		final Pattern pathPattern = (!recurse || pathFilterRegex == null) ? null : Pattern.compile(pathFilterRegex);
		final String remotePath = ctx.getProperty(FileTransfer.REMOTE_PATH).evaluateAttributeExpressions().getValue();

		logger.info(String.format("path : %s", path));
		logger.info(String.format("pathPattern : %s", pathPattern));
		
		// check if this directory path matches the PATH_FILTER_REGEX
		boolean pathFilterMatches = true;
		if (pathPattern != null) {
			Path reldir = path == null ? Paths.get(".") : Paths.get(path);
			if (remotePath != null) {
				reldir = Paths.get(remotePath).relativize(reldir);
			}
			if (reldir != null && !reldir.toString().isEmpty()) {
				if (!pathPattern.matcher(reldir.toString().replace("\\", "/")).matches()) {
					pathFilterMatches = false;
				}
			}
		}

		logger.info(String.format("pathFilterMatches : %s", pathFilterMatches));
		
		final FTPClient client = getClient(null);

		int count = 0;
		final FTPFile[] files;

		if (path == null || path.trim().isEmpty()) {
			files = client.listFiles(".");
		} else {
			files = client.listFiles(path);
		}
		if (files.length == 0 && path != null && !path.trim().isEmpty()) {
			// throw exception if directory doesn't exist
			final boolean cdSuccessful = setWorkingDirectory(path);
			if (!cdSuccessful) {
				throw new IOException("Cannot list files for non-existent directory " + path);
			}
		}

		for (final FTPFile file : files) {
			final String filename = file.getName();
			if (filename.equals(".") || filename.equals("..")) {
				continue;
			}

			if (ignoreDottedFiles && filename.startsWith(".")) {
				continue;
			}

			final File newFullPath = new File(path, filename);
			final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");

			if (file.isDirectory()) {
				logger.info("PATH: {} ",new Object[] { newFullForwardPath });
				// Repertoire
				if (maxDepth != null) {
					// Si la profondeur de recherche est définieoct@ve12
					int level = maxDepth.intValue() - 1;
					if (depth == level) {
						logger.info("depth == level");
						boolean matches= true;
						if (pathPattern != null) {
							Path reldir = path == null ? Paths.get(".") : Paths.get(newFullForwardPath);
							if (remotePath != null) {
								reldir = Paths.get(remotePath).relativize(reldir);
							}
							if (reldir != null && !reldir.toString().isEmpty()) {
								if (!pathPattern.matcher(reldir.toString().replace("\\", "/")).matches()) {
									matches = false;
								}
							}
						}
						if (pathPattern == null || matches) {
							try {
								logger.info("going into depth depth:{} and maxDepth: {} ",
										new Object[] { depth, maxDepth });
								listing.addAll(getListing(newFullForwardPath, depth + 1, maxResults - count, maxDepth));
							} catch (final IOException e) {
								logger.error("Unable to get listing from " + newFullForwardPath
										+ "; skipping this subdirectory");
								throw e;
							}
						}
					} else if (depth < level) {
						logger.info("depth < level");
						try {
							logger.info("going into depth depth:{} and maxDepth: {} ",
									new Object[] { depth, maxDepth });
							listing.addAll(getListing(newFullForwardPath, depth + 1, maxResults - count, maxDepth));
						} catch (final IOException e) {
							logger.error("Unable to get listing from " + newFullForwardPath
									+ "; skipping this subdirectory");
							throw e;
						}
					}
				} else if (recurse) {
					logger.info("MAxDepth  null and recurse = true");
					try {
						logger.info("Recurse mode depth depth:{}", new Object[] { depth });
						listing.addAll(getListing(newFullForwardPath, depth + 1, maxResults - count, maxDepth));
					} catch (final IOException e) {
						logger.error(
								"Unable to get listing from " + newFullForwardPath + "; skipping this subdirectory");
						throw e;
					}
				}

			}

			/*
			 * if ((file.isDirectory() && pathFilterMatches && maxDepth!=null &&
			 * depth<=maxDepth)) { try { logger.info(
			 * "going into depth depth:{} and maxDepth: {} ",new
			 * Object[]{depth,maxDepth});
			 * listing.addAll(getListing(newFullForwardPath, depth + 1,
			 * maxResults - count, maxDepth)); } catch (final IOException e) {
			 * logger.error("Unable to get listing from " + newFullForwardPath +
			 * "; skipping this subdirectory"); throw e; } }else{ if ((recurse
			 * && file.isDirectory()) ) { try { logger.info(
			 * "Recurse mode depth depth:{}",new Object[]{depth});
			 * listing.addAll(getListing(newFullForwardPath, depth + 1,
			 * maxResults - count, maxDepth)); } catch (final IOException e) {
			 * logger.error("Unable to get listing from " + newFullForwardPath +
			 * "; skipping this subdirectory"); throw e; } } }
			 */

			// if is not a directory and is not a link and it matches
			// FILE_FILTER_REGEX - then let's add it
			if (!file.isDirectory() && !file.isSymbolicLink() && pathFilterMatches) {
				if (pattern == null || pattern.matcher(filename).matches()) {
					logger.info(String.format("Ajout du fichier %s/%s", path, file.getName()));
					listing.add(newFileInfo(file, path));
					count++;
					logger.info(String.format("Nb fichiers retenus %s", count));
				}
			}

			if (count >= maxResults) {
				break;
			}
		}

		return listing;
	}

	private FileInfo newFileInfo(final FTPFile file, String path) {
		if (file == null) {
			return null;
		}
		final File newFullPath = new File(path, file.getName());
		final String newFullForwardPath = newFullPath.getPath().replace("\\", "/");
		StringBuilder perms = new StringBuilder();
		perms.append(file.hasPermission(FTPFile.USER_ACCESS, FTPFile.READ_PERMISSION) ? "r" : "-");
		perms.append(file.hasPermission(FTPFile.USER_ACCESS, FTPFile.WRITE_PERMISSION) ? "w" : "-");
		perms.append(file.hasPermission(FTPFile.USER_ACCESS, FTPFile.EXECUTE_PERMISSION) ? "x" : "-");
		perms.append(file.hasPermission(FTPFile.GROUP_ACCESS, FTPFile.READ_PERMISSION) ? "r" : "-");
		perms.append(file.hasPermission(FTPFile.GROUP_ACCESS, FTPFile.WRITE_PERMISSION) ? "w" : "-");
		perms.append(file.hasPermission(FTPFile.GROUP_ACCESS, FTPFile.EXECUTE_PERMISSION) ? "x" : "-");
		perms.append(file.hasPermission(FTPFile.WORLD_ACCESS, FTPFile.READ_PERMISSION) ? "r" : "-");
		perms.append(file.hasPermission(FTPFile.WORLD_ACCESS, FTPFile.WRITE_PERMISSION) ? "w" : "-");
		perms.append(file.hasPermission(FTPFile.WORLD_ACCESS, FTPFile.EXECUTE_PERMISSION) ? "x" : "-");

		FileInfo.Builder builder = new FileInfo.Builder().filename(file.getName()).fullPathFileName(newFullForwardPath)
				.directory(file.isDirectory()).size(file.getSize())
				.lastModifiedTime(file.getTimestamp().getTimeInMillis()).permissions(perms.toString())
				.owner(file.getUser()).group(file.getGroup());
		return builder.build();
	}

	private boolean setWorkingDirectory(final String path) throws IOException {
		client.changeWorkingDirectory(homeDirectory);
		return client.changeWorkingDirectory(path);
	}

	private boolean resetWorkingDirectory() throws IOException {
		return client.changeWorkingDirectory(homeDirectory);
	}

	private FTPClient getClient(final FlowFile flowFile) throws IOException {
		if (client != null) {
			String desthost = ctx.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
			if (remoteHostName.equals(desthost)) {
				// destination matches so we can keep our current session
				resetWorkingDirectory();
				return client;
			} else {
				// this flowFile is going to a different destination, reset
				// session
				close();
			}
		}

		final Proxy.Type proxyType = Proxy.Type.valueOf(ctx.getProperty(PROXY_TYPE).getValue());
		final String proxyHost = ctx.getProperty(PROXY_HOST).getValue();
		final Integer proxyPort = ctx.getProperty(PROXY_PORT).asInteger();
		FTPClient client;
		if (proxyType == Proxy.Type.HTTP) {
			client = new FTPHTTPClient(proxyHost, proxyPort, ctx.getProperty(HTTP_PROXY_USERNAME).getValue(),
					ctx.getProperty(HTTP_PROXY_PASSWORD).getValue());
		} else {
			client = new FTPClient();
			if (proxyType == Proxy.Type.SOCKS) {
				client.setSocketFactory(
						new SocksProxySocketFactory(new Proxy(proxyType, new InetSocketAddress(proxyHost, proxyPort))));
			}
		}
		this.client = client;
		client.setDataTimeout(ctx.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
		client.setDefaultTimeout(ctx.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
		client.setRemoteVerificationEnabled(false);

		final String remoteHostname = ctx.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
		this.remoteHostName = remoteHostname;
		InetAddress inetAddress = null;
		try {
			inetAddress = InetAddress.getByAddress(remoteHostname, null);
		} catch (final UnknownHostException uhe) {
		}

		if (inetAddress == null) {
			inetAddress = InetAddress.getByName(remoteHostname);
		}

		client.connect(inetAddress, ctx.getProperty(PORT).evaluateAttributeExpressions(flowFile).asInteger());
		this.closed = false;
		client.setDataTimeout(ctx.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
		client.setSoTimeout(ctx.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());

		final String username = ctx.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
		final String password = ctx.getProperty(PASSWORD).evaluateAttributeExpressions(flowFile).getValue();
		final boolean loggedIn = client.login(username, password);
		if (!loggedIn) {
			throw new IOException("Could not login for user '" + username + "'");
		}

		final String connectionMode = ctx.getProperty(CONNECTION_MODE).getValue();
		if (connectionMode.equalsIgnoreCase(CONNECTION_MODE_ACTIVE)) {
			client.enterLocalActiveMode();
		} else {
			client.enterLocalPassiveMode();
		}

		final String transferMode = ctx.getProperty(TRANSFER_MODE).evaluateAttributeExpressions(flowFile).getValue();
		final int fileType = (transferMode.equalsIgnoreCase(TRANSFER_MODE_ASCII)) ? FTPClient.ASCII_FILE_TYPE
				: FTPClient.BINARY_FILE_TYPE;
		if (!client.setFileType(fileType)) {
			throw new IOException("Unable to set transfer mode to type " + transferMode);
		}

		this.homeDirectory = client.printWorkingDirectory();
		return client;
	}

}