/**
 *  Copyright 2011 Zuse Institute Berlin
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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

/**
 * A class loader capable of loading plugin classes from a given plugin
 * directory.
 * 
 * <p>
 * It allows for an efficient retrieval of plugins by means of an interface.
 * Three requirements have to be met:
 * <ul>
 * <li>the policy needs to be assignable from any of the given policy interfaces
 * </li>
 * <li>the policy needs to be in the plug-in directory together with all its
 * dependencies.</li>
 * </ul>
 * </p>
 * 
 * @author Jan Stender, stender@zib.de
 * @author Nico Kruber, kruber@zib.de
 */
public class PluginClassLoader extends ClassLoader {

    private final Map<String, Class<?>> cache = new HashMap<String, Class<?>>();

    private final Map<Class<?>, List<Class<?>>> pluginMap = new HashMap<Class<?>, List<Class<?>>>();

    private final Class<?>[] policyInterfaces;

    private File policyDir;

    private File[] jarFiles = new File[0];

    private List<File> subDirs = new LinkedList<File>();

    private static final Pattern REMOVE_JAR_EXT = Pattern.compile("\\.jar$");

    private static final Pattern REMOVE_CLASS_EXT = Pattern.compile("\\.class$");

    /**
     * Instantiates a new policy class loader.
     * 
     * @param policyDirPath
     *            the path for the directory with all (compiled) plugins
     * @param policyInterfaces
     *            the policy interfaces
     * @throws IOException
     *             if an error occurs while initialising the policy interfaces
     *             or built-in policies
     */
    public PluginClassLoader(String policyDirPath, Class<?>[] policyInterfaces)
            throws IOException {
        this.policyDir = new File(policyDirPath);
        this.policyInterfaces = policyInterfaces;
        init();
    }

    /**
     * Initialises the class loader. Each class in the
     * directory is loaded and checked for assignability to one of the given
     * policy interfaces. If the check is successful, the class is added to a
     * map, from which it can be retrieved.
     * 
     * @throws IOException
     *             if an I/O error occurs while loading any of the classes
     */
    public void init() throws IOException {
        if (policyDir.exists()) {
            // retrieve all policies from class files
            File[] classFiles = policyDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getAbsolutePath().endsWith(".class");
                }
            });
            
            for (File cls : classFiles) {
                String filename = REMOVE_CLASS_EXT.matcher(cls.getName()).replaceAll(File.separator);
                addPluginSubdir(filename);
                try {
                    String className = cls.getAbsolutePath().substring(
                        policyDir.getAbsolutePath().length() + 1,
                        cls.getAbsolutePath().length() - ".class".length()).replace('/', '.');
                    if (cache.containsKey(className)) {
                        continue;
                    }
                    
                    // load the class
                    Class<?> clazz = loadFromStream(new FileInputStream(cls));
                    
                    // check whether the class refers to a policy; if so,
                    // cache it
                    checkClass(clazz);
                    
                } catch (LinkageError err) {
                    // ignore linkage errors
                } catch (Exception err) {
                    err.printStackTrace(); // TODO: remove
                }
            }
            
            // get all JAR files
            jarFiles = policyDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getAbsolutePath().endsWith(".jar");
                }
            });

            // retrieve all policies from JAR files, add appropriate subdirs to
            // the list of directories
            for (File jar : jarFiles) {
                String subdir = REMOVE_JAR_EXT.matcher(jar.getName()).replaceAll(File.separator);
                addPluginSubdir(subdir);
                JarFile jarFile = new JarFile(jar);

                Enumeration<JarEntry> entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    if (entry.getName().endsWith(".class")) {
                        try {
                            // load the class
                            Class<?> clazz = loadFromStream(jarFile.getInputStream(entry));

                            // check whether the class refers to a policy; if
                            // so, cache it
                            checkClass(clazz);
                        } catch (IOException err) {
                            err.printStackTrace(); // TODO: remove
                        } catch (LinkageError err) {
                            // ignore
                        }
                    }
                }
            }
        }
    }

    /**
     * Adds the sub-directory to the class path as well as all .jar files in
     * this directory.
     * 
     * @param pluginName
     *            the name of the plugin (determines the sub-directory's name)
     */
    private void addPluginSubdir(String pluginName) {
        File clsFile = new File(policyDir + File.separator + pluginName);
        if (clsFile.isDirectory()) {
            // add the sub-directory itself
            subDirs.add(clsFile);

            // add all JAR files in the sub-directory
            File[] jarsInSubDir = clsFile.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getAbsolutePath().endsWith(".jar");
                }
            });
            subDirs.addAll(Arrays.asList(jarsInSubDir));
        }
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

        if (!policyDir.exists()) {
            throw new ClassNotFoundException("the plug-in policy directory does not exist");
        }

        // if the class could not be loaded by the system class loader, try
        // to load it from an external JAR file
        URL[] urls = new URL[subDirs.size() + jarFiles.length];
        try {
            int i = 0;
            // add the plugins' directories for dependencies
            for (File subdir: subDirs) {
                urls[i++] = subdir.toURI().toURL();
            }
            for (File jarFile: jarFiles) {
                // add the plugin's directory for dependencies
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
     * Returns a list of all plugins for a given interface.
     * 
     * @param pluginInterface
     *            the policy interface
     * 
     * @return a list of plugin classes where each is assignable from the given
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

        // then try to get the resource from a plugin's folder
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

        // then try to get the resource from a plugin's folder
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
     * Check whether the class matches any of the policy interfaces and adds
     * the class to the appropriate list(s) of assignable classes.
     * 
     * @param clazz the class to check
     */
    private void checkClass(Class<?> clazz) {
        for (Class<?> ifc : policyInterfaces) {
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
