package org.tarantool.facade;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;

public class User {
	int id = 123;
	long phone = 880012345678L;
	double point = 0.333d;
	float iq = 0.81f;
	short height = 188;
	BigInteger lifeFormId = BigInteger.valueOf(Long.MAX_VALUE);
	BigDecimal salary = BigDecimal.valueOf(123.456);
	Date birthday = new Date();
	String name = "John Smith";
	byte[] sign = { 1, 2, 3, 4 };
	boolean male = true;
	URL site;

	public User() throws MalformedURLException {
		site = new URL("http://localhost:8080/index.html");
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((birthday == null) ? 0 : birthday.hashCode());
		result = prime * result + height;
		result = prime * result + id;
		result = prime * result + Float.floatToIntBits(iq);
		result = prime * result + ((lifeFormId == null) ? 0 : lifeFormId.hashCode());
		result = prime * result + (male ? 1231 : 1237);
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (int) (phone ^ (phone >>> 32));
		long temp;
		temp = Double.doubleToLongBits(point);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((salary == null) ? 0 : salary.hashCode());
		result = prime * result + Arrays.hashCode(sign);
		result = prime * result + ((site == null) ? 0 : site.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		User other = (User) obj;
		if (birthday == null) {
			if (other.birthday != null)
				return false;
		} else if (!birthday.equals(other.birthday))
			return false;
		if (height != other.height)
			return false;
		if (id != other.id)
			return false;
		if (Float.floatToIntBits(iq) != Float.floatToIntBits(other.iq))
			return false;
		if (lifeFormId == null) {
			if (other.lifeFormId != null)
				return false;
		} else if (!lifeFormId.equals(other.lifeFormId))
			return false;
		if (male != other.male)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (phone != other.phone)
			return false;
		if (Double.doubleToLongBits(point) != Double.doubleToLongBits(other.point))
			return false;
		if (salary == null) {
			if (other.salary != null)
				return false;
		} else if (!salary.equals(other.salary))
			return false;
		if (!Arrays.equals(sign, other.sign))
			return false;
		if (site == null) {
			if (other.site != null)
				return false;
		} else if (!site.equals(other.site))
			return false;
		return true;
	}

}