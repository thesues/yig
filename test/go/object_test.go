package _go

import (
	. "github.com/journeymidnight/yig/test/go/lib"
	"net/http"
	"testing"
	"time"
)

func Test_Object_Prepare(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
}

func Test_PutObject(t *testing.T) {
	sc := NewS3()
	err := sc.PutObject(TEST_BUCKET, TEST_KEY, []byte(TEST_VALUE))
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")
}

func Test_HeadObject(t *testing.T) {
	sc := NewS3()
	err := sc.HeadObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("HeadBucket err:", err)
	}
	t.Log("HeadObject Success!")
}

func Test_GetObject(t *testing.T) {
	sc := NewS3()
	v, err := sc.GetObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	if v != TEST_VALUE {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TEST_VALUE)
	}
	t.Log("GetObject Success value:", v)
}

func Test_DeleteObject(t *testing.T) {
	sc := NewS3()
	err := sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("DeleteObject err:", err)
	}
	err = sc.HeadObject(TEST_BUCKET, TEST_KEY)
	if err == nil {
		t.Fatal("HeadObject err:", err)
	}
	t.Log("DeleteObject Success!")
}

func Test_SmallFile(t *testing.T) {
	sc := NewS3()
	var tt []byte
	for i := 0; i < 1024*1024*10; i++ {
		tt = append(tt, 'a')
	}

	for i := 0; i < 100; i++ {
		err := sc.PutObject(TEST_BUCKET, TEST_KEY, append(tt, byte(i)))
		if err != nil {
			t.Fatal("PutObject err:", err)
		}
	}
	t.Log("PutObjects Success!")

	//for i:=0; i<100 ;i++ {
	//	v, err := sc.GetObject(TEST_BUCKET, TEST_KEY+strconv.Itoa(i))
	//	if err != nil {
	//		t.Fatal("GetObject err:", err)
	//	}
	//	if v != TEST_VALUE+strconv.Itoa(i) {
	//		t.Fatal("GetObject err: value is:", v, ", but should be:", TEST_VALUE)
	//	}
	//}
	//t.Log("GetObjects Success!")

	for i := 0; i < 100; i++ {
		err := sc.DeleteObject(TEST_BUCKET, TEST_KEY)
		if err != nil {
			t.Fatal("DeleteObject err:", err)
		}
	}
	t.Log("DeleteObjects Success!")

	for i := 0; i < 100; i++ {
		err := sc.DeleteObject(TEST_BUCKET, TEST_KEY)
		if err != nil {

		}
	}
	err := sc.GetBucketMetrics(TEST_BUCKET)
	if err != nil {
		t.Log(err)
	}

}

func Test_PreSignedGetObject(t *testing.T) {
	sc := NewS3()
	err := sc.PutObject(TEST_BUCKET, TEST_KEY, []byte(TEST_VALUE))
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	url, err := sc.GetObjectPreSigned(TEST_BUCKET, TEST_KEY, 5*time.Second)
	if err != nil {
		t.Fatal("GetObjectPreSigned err:", err)
	}
	t.Log("url:", url)
	// After set presign
	statusCode, data, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))

	//After 5 second
	time.Sleep(5 * time.Second)
	statusCode, _, err = HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}
	t.Log("PreSignedGetObject Success.")
}

func Test_Object_End(t *testing.T) {
	sc := NewS3()
	err := sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}
