package org.tarantool.facade.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Sets position of element in tuple and position in every index where used
 * 
 * @author dgreen
 * @version $Id: $
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Field {
	int value();

	Index[] index() default {};
}
