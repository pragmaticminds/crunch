package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.exceptions.CloneFailedException;

import java.io.*;
import java.util.HashMap;

/**
 * Clones serializable objects
 * <p>
 * extracted from org.apache.flink.util.InstantiationUtil
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 02.08.2018
 */
public final class ClonerUtil {
    /** hidden constructor */
    private ClonerUtil() {
        throw new UnsupportedOperationException("this class should never be initialized!");
    }

    /**
     * Clones the given serializable object using Java serialization.
     *
     * @param obj Object to clone
     * @param <T> Type of the object to clone
     * @return Cloned object
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static <T extends Serializable> T clone(T obj) {
        if (obj == null) {
            return null;
        } else {
            try {
                return clone(obj, obj.getClass().getClassLoader());
            } catch (IOException ex) {
                throw new CloneFailedException(ex);
            }
        }
    }

    /**
     * Clones the given serializable object using Java serialization, using the given classloader to
     * resolve the cloned classes.
     *
     * @param obj         Object to clone
     * @param classLoader The classloader to resolve the classes during deserialization.
     * @param <T>         Type of the object to clone
     * @return Cloned object
     */
    public static <T extends Serializable> T clone(T obj, ClassLoader classLoader) throws IOException {
        if (obj == null) {
            return null;
        } else {
            final byte[] serializedObject = serializeObject(obj);
            return deserializeObject(serializedObject, classLoader);
        }
    }

    /**
     * Serializes any serializable object to a byte array
     *
     * @param o ingoing object
     * @return byte array representing the serialized object
     * @throws IOException on serialization fails
     */
    public static byte[] serializeObject(Object o) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            return baos.toByteArray();
        }
    }

    /**
     * Deserializes any serialized object from a byte array, that was serialized by the method serilizeObject before
     *
     * @param bytes representing the serialized object
     * @param cl    is the class loader
     * @param <T>   out type
     * @return the deserialized object
     * @throws IOException            should never happen
     * @throws ClassNotFoundException happens if the given class was not found
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserializeObject(byte[] bytes, ClassLoader cl) {
        final ClassLoader old = Thread.currentThread().getContextClassLoader();
        try (ObjectInputStream oois = new ClassLoaderObjectInputStream(new ByteArrayInputStream(bytes), cl)) {
            Thread.currentThread().setContextClassLoader(cl);
            return (T) oois.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            throw new CloneFailedException(ex);
        } finally {
            Thread.currentThread().setContextClassLoader(old);
        }
    }

    /**
     * Copied from flinks InstantionUtils.ClassLoaderObjectInputStream.
     * <p>
     * A custom ObjectInputStream that can load classes using a specific ClassLoader.
     */
    public static class ClassLoaderObjectInputStream extends ObjectInputStream {

        private static final HashMap<String, Class<?>> primitiveClasses = new HashMap<>(9);

        static {
            primitiveClasses.put("boolean", boolean.class);
            primitiveClasses.put("byte", byte.class);
            primitiveClasses.put("char", char.class);
            primitiveClasses.put("short", short.class);
            primitiveClasses.put("int", int.class);
            primitiveClasses.put("long", long.class);
            primitiveClasses.put("float", float.class);
            primitiveClasses.put("double", double.class);
            primitiveClasses.put("void", void.class);
        }

        protected final ClassLoader classLoader;

        // ------------------------------------------------

        public ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            if (classLoader != null) {
                String name = desc.getName();
                try {
                    return Class.forName(name, false, classLoader);
                } catch (ClassNotFoundException ex) {
                    // check if class is a primitive class
                    Class<?> cl = primitiveClasses.get(name);
                    if (cl != null) {
                        // return primitive class
                        return cl;
                    } else {
                        // throw ClassNotFoundException
                        throw ex;
                    }
                }
            }

            return super.resolveClass(desc);
        }
    }
}
