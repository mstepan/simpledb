use std::fs::{create_dir_all, metadata, remove_dir_all};
use std::panic;
use std::sync::Mutex;

static DIR_NO: Mutex<u32> = Mutex::new(0);

pub struct FSTestUtil {
    test_dir: String
}

impl FSTestUtil {
    pub fn new(test_dir_root: &str) -> Self {
        let mut dir_no = DIR_NO.lock().unwrap();

        *dir_no = *dir_no + 1;

        return Self {
            test_dir: format!("{}-{}", test_dir_root, *dir_no)
        };
    }

    pub fn run_test<T>(&mut self, test: T) -> ()
    where
        T: FnOnce(&str) -> () + panic::UnwindSafe,
    {
        // Allow only one test at a time, otherwise we will have spurious issues b/c
        // the same 'test_dir' directory is used
        // let _guard = self.lock.lock().unwrap();
        Self::setup(&self.test_dir);
        let result = panic::catch_unwind(|| test(&self.test_dir));
        Self::teardown(&self.test_dir);
        assert!(result.is_ok())
    }

    fn setup(dir: &str) {
        if metadata(dir).is_ok() {
            remove_dir_all(dir).expect("Can't delete 'db_test' directory in setup");
        }

        create_dir_all(dir).expect("Can't create 'db_test' directory in setup")
    }

    fn teardown(dir: &str) {
        remove_dir_all(dir).expect("Can't delete 'db_test' directory in teardown")
    }
}
