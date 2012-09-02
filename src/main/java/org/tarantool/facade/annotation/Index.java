package org.tarantool.facade.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Sets element position in specified index
 * 
 * @author dgreen
 * @version $Id: $
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface Index {
	int indexNo();

	int fieldNo();
}
