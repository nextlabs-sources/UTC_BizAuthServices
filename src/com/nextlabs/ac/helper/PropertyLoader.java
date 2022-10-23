package com.nextlabs.ac.helper;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class PropertyLoader
{
	private static final Log LOG = LogFactory.getLog(PropertyLoader.class);
    public static Properties loadProperties (String name)
    {
        if (name == null)
            throw new IllegalArgumentException ("null input: name");
        
        if (name.startsWith ("/"))
            name = "."+name;
        
        Properties result = null;
        try{ 
            File file = new File(name);
            LOG.debug("Properties File Path:: "+file.getAbsolutePath());        	
            System.out.println("Properties File Path:: "+file.getAbsolutePath());
            if (file != null){
            	FileInputStream fis = new FileInputStream(file);
                result = new Properties();
                result.load (fis); // Can throw IOException
            }
        }
        catch (Exception e){
        	LOG.error("Error parsing properties file ", e);
            result = null;
        }
        return result;
    }    
} // End of class