package recommend.demo.utils;

public class RetBuilder<T> {
    class RetBean {
        int code;
        String msg = "success";
        T data;
    }

    RetBean retBean = null;

    public RetBuilder() {
        retBean = new RetBean();
    }

    public RetBean build() {
        RetBean ret = this.retBean;
        this.retBean = null;
        return ret;
    }

    public RetBuilder<T> code(int code) {
        this.retBean.code = code;
        return this;
    }

    public RetBuilder<T> msg(String msg) {
        this.retBean.msg = msg;
        return this;
    }

    public RetBuilder<T> data(T data) {
        this.retBean.data = data;
        return this;
    }
}
