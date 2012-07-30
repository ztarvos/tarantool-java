package org.tarantool.facade;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;

public class User {
	int id=123;
	long phone=880012345678L;
	double point=0.333d;
	float iq=0.81f;
	short height=188;
	BigInteger lifeFormId=BigInteger.valueOf(Long.MAX_VALUE);
	BigDecimal salary=BigDecimal.valueOf(123.456);
	Date birthday=new Date();
	String name="John Smith";
	byte[] sign={1,2,3,4};
	boolean male=true;
	URL site;
	public User() throws MalformedURLException {
		site=new URL("http://localhost:8080/index.html");
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public long getPhone() {
		return phone;
	}

	public void setPhone(long phone) {
		this.phone = phone;
	}

	public double getPoint() {
		return point;
	}

	public void setPoint(double point) {
		this.point = point;
	}

	public float getIq() {
		return iq;
	}

	public void setIq(float iq) {
		this.iq = iq;
	}

	public short getHeight() {
		return height;
	}

	public void setHeight(short height) {
		this.height = height;
	}

	public BigInteger getLifeFormId() {
		return lifeFormId;
	}

	public void setLifeFormId(BigInteger lifeFormId) {
		this.lifeFormId = lifeFormId;
	}

	public BigDecimal getSalary() {
		return salary;
	}

	public void setSalary(BigDecimal salary) {
		this.salary = salary;
	}

	public Date getBirthday() {
		return birthday;
	}

	public void setBirthday(Date birthday) {
		this.birthday = birthday;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public byte[] getSign() {
		return sign;
	}

	public void setSign(byte[] sign) {
		this.sign = sign;
	}

	public boolean isMale() {
		return male;
	}

	public void setMale(boolean male) {
		this.male = male;
	}

	public URL getSite() {
		return site;
	}

	public void setSite(URL site) {
		this.site = site;
	}

}