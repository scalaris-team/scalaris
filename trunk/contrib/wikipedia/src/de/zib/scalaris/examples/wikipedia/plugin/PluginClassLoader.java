/**
 *  Copyright 2011-2013 Zuse Institute Berlin
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.zib.scalaris.examples.wikipedia.plugin;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

/**
 * A class loader capable of loading plug-in classes from a given plug-in
 * directory.
 * 
 * <p>
 * It allows for an efficient retrieval of plug-ins by means of an interface.
 * Three requirements have to be met:
 * <ul>
 * <li>the plug-in needs to be assignable from any of the given plug-in interfaces
 * </li>
 * <li>the policy needs to be in the plug-in directory, its dependencies need to
 * be placed in the <tt>%lt;pluginname&gt;</tt> sub-directory of the plug-in
 * directory.</li>
 * </ul>
 * </p>
 * 
 * @author Jan Stender, stender@zib.de
 * @author Nico Kruber, kruber@zib.de
 */
public class PluginClassLoader extends ClassLoader {

    private final Map<String, Class<?>> cache = new HashMap<String, Class<?>>();

    private final Map<Class<?>, List<Class<?>>> pluginMap = new HashMap<Class<?>, List<Class<?>>>();

    private final Class<?>[] pluginInterfaces;

    private File pluginDir;

    private File[] jarFiles = new File[0];

    private List<File> subDirs = new LinkedList<File>();

    private static final Pattern REMOVE_JAR_EXT = Pattern.compile("\\.jar$");

    private static final Pattern REMOVE_JAVA_EXT = Pattern.compile("\\.java$");

    private static final Pattern REMOVE_CLASS_EXT = Pattern.compile("\\.class$");

    /**
     * Instantiates a new plug-in class loader.
     * 
     * @param pluginDirPath
     *            the path for the directory with all (compiled) plug-ins
     * @param pluginInterfaces
     *            the plug-in interfaces (any plug-in implementing any of these
     *            interfaces will be loaded)
     * @throws IOException
     *             if an error occurs while initialising the plug-in interfaces
     */
    public PluginClassLoader(String pluginDirPath, Class<?>[] pluginInterfaces)
            throws IOException {
        this.pluginDir = new File(pluginDirPath);
        this.pluginInterfaces = pluginInterfaces;
        init();
    }
    
    /**
     * Initialises the class loader. Each class in the
     * directory is loaded and checked for assignability to one of the given
     * plug-in interfaces. If the check is successful, the class is added to a
     * map, from which it can be retrieved.
     * 
     * @throws IOException
     *             if an I/O error occurs while loading any of the classes
     */
    public void init() throws IOException {
        if (pluginDir.exists()) {
            // get all Java files recursively
            File[] javaFiles = pluginDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getPath().endsWith(".java");
                }
            });
            
            // compile all Java files
            if (javaFiles.length != 0) {
                JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
                if (compiler != null) {
                    List<File> pluginJars = new LinkedList<File>();
                    for (File javaFile: javaFiles) {
                        String filename = REMOVE_JAVA_EXT.matcher(javaFile.getName()).replaceAll("");
                        pluginJars.addAll(Arrays.asList(getPluginJars(filename)));
                    } 
                    String cp = buildClassPath(pluginJars);
                    List<String> options = Arrays.asList("-cp", cp);
                    StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);

                    Iterable<? extends JavaFileObject> compilationUnits = fileManager
                            .getJavaFileObjectsFromFiles(Arrays.asList(javaFiles));
                    if (!compiler.getTask(null, fileManager, null, options, null, compilationUnits).call()) {
                        // note: an error is already printed to the log
                    }

                    fileManager.close();
                }
            }
            
            
            // retrieve all policies from class files
            File[] classFiles = pluginDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getPath().endsWith(".class");
                }
            });
            
            for (File cls : classFiles) {
                String filename = REMOVE_CLASS_EXT.matcher(cls.getName()).replaceAll("");
                subDirs.addAll(Arrays.asList(getPluginJars(filename)));
                try {
                    String className = cls.getAbsolutePath().substring(
                        pluginDir.getAbsolutePath().length() + 1,
                        cls.getAbsolutePath().length() - ".class".length()).replace(File.separatorChar, '.');
                    if (cache.containsKey(className)) {
                        continue;
                    }
                    
                    // load the class
                    Class<?> clazz = loadFromStream(new FileInputStream(cls));
                    
                    // check whether the class refers to a plug-in; if so,
                    // cache it
                    checkClass(clazz);
                    
                } catch (LinkageError err) {
                    // ignore linkage errors
                } catch (Exception err) {
                    err.printStackTrace(); // TODO: remove
                }
            }
            
            // get all JAR files
            jarFiles = pluginDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getPath().endsWith(".jar");
                }
            });

            // retrieve all policies from JAR files, add appropriate subdirs to
            // the list of directories
            for (File jar : jarFiles) {
                String subdir = REMOVE_JAR_EXT.matcher(jar.getName()).replaceAll("");
                subDirs.addAll(Arrays.asList(getPluginJars(subdir)));
                JarFile jarFile = new JarFile(jar);

                Enumeration<JarEntry> entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    if (entry.getName().endsWith(".class")) {
                        try {
                            // load the class
                            Class<?> clazz = loadFromStream(jarFile.getInputStream(entry));

                            // check whether the class refers to a plug-in; if
                            // so, cache it
                            checkClass(clazz);
                        } catch (IOException err) {
                            err.printStackTrace(); // TODO: remove
                        } catch (LinkageError err) {
                            // ignore
                        }
                    }
                }
                jarFile.close();
            }
        }
    }

    /**
     * Uses the class path defined in the system class path, used by this class'
     * class loader and system class loader to build a class path for
     * compilation. Adds all given jars as well.
     * 
     * @param addJars
     *            jar files to add to the class path
     * 
     * @return the class path as a string
     */
    private String buildClassPath(Collection<File> addJars) {
        StringBuilder cp = new StringBuilder();
        cp.append(System.getProperty("java.class.path" ));
        
        for (ClassLoader cl: new ClassLoader[] {this.getClass().getClassLoader(), getSystemClassLoader()}) {
            if (cl != null && cl instanceof URLClassLoader) {
                // note: we don't create new class loaders here!
                @SuppressWarnings("resource")
                URLClassLoader clu = (URLClassLoader) cl;
                for (URL url: clu.getURLs()) {
                    cp.append(File.pathSeparatorChar);
                    cp.append(url.getPath());
                }
            }
        }
        for (File jarFile: addJars) {
            cp.append(File.pathSeparatorChar);
            cp.append(jarFile.getAbsolutePath());
        }
        return cp.toString();
    }

    /**
     * Gets all jar files in the sub-directory the given plugin can place
     * dependencies in.
     * 
     * @param pluginName
     *            the name of the plug-in (determines the sub-directory's name)
     * 
     * @return all jar files (may be an empty array)
     */
    private File[] getPluginJars(String pluginName) {
        File clsFile = new File(pluginDir + File.separator + pluginName);
        if (clsFile.isDirectory()) {
            // add the sub-directory itself
            subDirs.add(clsFile);

            // add all JAR files in the sub-directory
            File[] jarsInSubDir = clsFile.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getPath().endsWith(".jar");
                }
            });
            return jarsInSubDir;
        }
        return new File[0];
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // first, check whether the class is cached
        if (cache.containsKey(name)) {
            return cache.get(name);
        }

        // if not cached, try to resolve the class by means of the system
        // class loader
        try {
            return getClass().getClassLoader().loadClass(name);
        } catch (ClassNotFoundException exc) {
        }

        if (!pluginDir.exists()) {
            throw new ClassNotFoundException("the plug-in directory does not exist");
        }

        // if the class could not be loaded by the system class loader, try
        // to load it from an external JAR file
        URL[] urls = new URL[subDirs.size() + jarFiles.length];
        try {
            int i = 0;
            // add the plug-ins' directories for dependencies
            for (File subdir: subDirs) {
                urls[i++] = subdir.toURI().toURL();
            }
            for (File jarFile: jarFiles) {
                // add the plug-in's directory for dependencies
                urls[i++] = jarFile.toURI().toURL();
            }
        } catch (MalformedURLException err) {
        }
        
        return new URLClassLoader(urls) {
            @Override
            public URL getResource(String name) {
                URL resource = super.getResource(name);
                if (resource != null) {
                    return resource;
                }
                return PluginClassLoader.this.getResource(name);
            }

            @Override
            public InputStream getResourceAsStream(String name) {
                InputStream stream = super.getResourceAsStream(name);
                if (stream != null) {
                    return stream;
                }
                return PluginClassLoader.this.getResourceAsStream(name);
            }

        }.loadClass(name);
    }

    /**
     * Returns a list of all plug-ins for a given interface.
     * 
     * @param pluginInterface
     *            the plug-in interface
     * 
     * @return a list of plug-in classes where each is assignable from the given
     *         interface
     */
    public List<Class<?>> getClasses(Class<?> pluginInterface) {
        return pluginMap.get(pluginInterface);
    }

    @Override
    public URL getResource(String name) {
        // first, try to get the resource from the parent class loader
        URL resource = super.getResource(name);
        if (resource != null) {
            return resource;
        }

        // then try to get the resource from a plug-in's folder
        for (File subDir: subDirs) {
            File file = new File(subDir.getAbsolutePath() + File.separator + name);
            if (file.exists()) {
                try {
                    return file.toURI().toURL();
                } catch (MalformedURLException e) {
                    return null;
                }
            }
        }

        return null;
    }

    @Override
    public InputStream getResourceAsStream(String name) {

        // first, try to get the stream from the parent class loader
        InputStream stream = super.getResourceAsStream(name);
        if (stream != null) {
            return stream;
        }

        // then try to get the resource from a plug-in's folder
        for (File subDir: subDirs) {
            File file = new File(subDir.getAbsolutePath() + File.separator + name);
            try {
                return new FileInputStream(file);
            } catch (FileNotFoundException exc) {
                return null;
            }
        }

        return null;
    }

    private Class<?> loadFromStream(InputStream in) throws IOException {
        // load the binary class content
        byte[] classData = new byte[in.available()];
        in.read(classData);
        in.close();

        Class<?> clazz = defineClass(null, classData, 0, classData.length);
        cache.put(clazz.getName(), clazz);

        return clazz;
    }

    /**
     * Check whether the class matches any of the plug-in interfaces and adds
     * the class to the appropriate list(s) of assignable classes.
     * 
     * @param clazz the class to check
     */
    private void checkClass(Class<?> clazz) {
        for (Class<?> ifc : pluginInterfaces) {
            if (ifc.isAssignableFrom(clazz)) {
                List<Class<?>> assignableClasses = pluginMap.get(ifc);
                if (assignableClasses == null) {
                    assignableClasses = new LinkedList<Class<?>>();
                    pluginMap.put(ifc, assignableClasses);
                }
                assignableClasses.add(clazz);
            }
        }
    }
}
